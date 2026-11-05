import {
  AgentSessionImportSource,
  EventId,
  MessageId,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import * as HostProcess from "@t3tools/shared/HostProcess";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as TurnItemPositionStore from "../orchestration-v2/TurnItemPositionStore.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

export const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const SYNC_EVENT_PREFIX = "agent-session-sync:v1";
const TRANSCRIPT_POLL_INTERVAL = Duration.seconds(3);
const MAX_WATCHED_THREADS = 32;
// A transcript missing from its expected paths is searched for at most once a minute.
const TRANSCRIPT_SEARCH_EVERY_POLLS = 20;
// A long catch-up reads at most this much per sync and continues on the next poll.
const MAX_SYNC_BYTES = 16 * 1024 * 1024;
const READ_CHUNK_BYTES = 1024 * 1024;
// Larger records are tool output, not conversation; they are skipped without being held.
const MAX_TRANSCRIPT_LINE_BYTES = 32 * 1024 * 1024;
// Claude stamps a prompt after T3 marks the turn started and can flush records just after it settles.
const T3_TURN_LEAD_MS = 2_000;
const T3_TURN_GRACE_MS = 5_000;
const WORKING_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "starting",
  "running",
  "waiting",
]);

const decodeImportedTranscripts = Schema.decodeUnknownOption(
  Schema.Struct({ importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)) }),
);
const decodeClaudeLine = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      type: Schema.optional(Schema.String),
      uuid: Schema.optional(Schema.String),
      timestamp: Schema.optional(Schema.String),
      isSidechain: Schema.optional(Schema.Boolean),
      isMeta: Schema.optional(Schema.Boolean),
      isCompactSummary: Schema.optional(Schema.Boolean),
      message: Schema.optional(
        Schema.Struct({
          content: Schema.optional(
            Schema.Union([
              Schema.String,
              Schema.Array(
                Schema.Struct({
                  type: Schema.optional(Schema.String),
                  text: Schema.optional(Schema.String),
                }),
              ),
            ]),
          ),
        }),
      ),
    }),
  ),
);

export function importedMessageEvents(input: {
  readonly threadId: ThreadId;
  readonly prefix: string;
  readonly messageKey: string;
  readonly itemKey: string;
  readonly ordinal: number;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const messageId = MessageId.make(`${input.threadId}:${input.messageKey}`);
  const turnItemId = TurnItemId.make(
    `${input.prefix}:turn-item:${input.threadId}:${input.itemKey}`,
  );
  const at = DateTime.makeUnsafe(input.message.createdAt);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: input.ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${input.prefix}:message:${input.threadId}:${input.itemKey}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    {
      id: EventId.make(`${input.prefix}:turn-item:${input.threadId}:${input.itemKey}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

interface TranscriptEntry {
  readonly uuid: string | undefined;
  readonly atMs: number | null;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage | null;
  readonly isPrompt: boolean;
}

function parseClaudeTranscriptLine(line: string): TranscriptEntry | null {
  if (line.trim().length === 0) return null;
  const record = Option.getOrUndefined(decodeClaudeLine(line));
  if (record === undefined || record.isSidechain === true) return null;
  if (record.type !== "user" && record.type !== "assistant") return null;
  const atMs = record.timestamp === undefined ? NaN : Date.parse(record.timestamp);
  const message =
    record.timestamp === undefined
      ? null
      : AgentSessionScanner.claudeTranscriptMessage(record, record.timestamp);
  return {
    uuid: record.uuid,
    atMs: Number.isFinite(atMs) ? atMs : null,
    message,
    // Tool results carry no text and interrupt markers continue the interrupted turn.
    isPrompt:
      message?.role === "user" &&
      record.isMeta !== true &&
      !message.text.startsWith("[Request interrupted"),
  };
}

interface T3TurnEvidence {
  /** Claude message uuids T3 recorded for its own turns. */
  readonly nativeIds: ReadonlySet<string>;
  readonly windows: ReadonlyArray<{ readonly startMs: number; readonly endMs: number }>;
}

type ExternalMessage = AgentSessionScanner.AgentSessionThreadMessage & { readonly uuid: string };

/**
 * Collects conversation messages from segments T3 did not run. A segment starts at a user prompt;
 * it is T3's if it holds a uuid T3 recorded, or its prompt falls inside one of T3's provider
 * turns. A segment open at the end of one sync continues in the next with `openOwnedByT3`.
 */
function externalMessageCollector(input: {
  readonly evidence: T3TurnEvidence;
  readonly importedThroughMs: number;
  readonly openOwnedByT3: boolean;
}) {
  const messages: Array<ExternalMessage> = [];
  let segment: Array<ExternalMessage> = [];
  let ownedByT3 = input.openOwnedByT3;
  const close = () => {
    if (!ownedByT3) messages.push(...segment);
    segment = [];
  };
  return {
    add: (entry: TranscriptEntry) => {
      if (entry.isPrompt) {
        close();
        const promptAtMs = entry.atMs;
        ownedByT3 =
          promptAtMs !== null &&
          input.evidence.windows.some(
            (window) =>
              promptAtMs >= window.startMs - T3_TURN_LEAD_MS &&
              promptAtMs <= window.endMs + T3_TURN_GRACE_MS,
          );
      }
      if (entry.uuid !== undefined && input.evidence.nativeIds.has(entry.uuid)) ownedByT3 = true;
      if (
        !ownedByT3 &&
        entry.message !== null &&
        entry.uuid !== undefined &&
        entry.atMs !== null &&
        entry.atMs > input.importedThroughMs
      ) {
        segment.push({ ...entry.message, uuid: entry.uuid });
      }
    },
    finish: () => {
      close();
      return { messages, openOwnedByT3: ownedByT3 };
    },
  };
}

export type ImportedTranscriptSyncResult =
  | { readonly _tag: "NotSyncable" }
  | { readonly _tag: "Busy" }
  | { readonly _tag: "Unchanged"; readonly source: AgentSessionImportSource }
  | {
      readonly _tag: "Synced";
      readonly appendedCount: number;
      readonly source: AgentSessionImportSource;
      /** False when the read stopped at the per-sync budget before the end of the file. */
      readonly caughtUp: boolean;
    };

export interface ImportedTranscriptSyncShape {
  /**
   * Appends turns another Claude client wrote to an imported thread's transcript. `source`
   * forces a read of that transcript even when its identity matches the recorded one.
   */
  readonly syncThread: (
    threadId: ThreadId,
    options?: { readonly source?: AgentSessionImportSource },
  ) => Effect.Effect<ImportedTranscriptSyncResult>;
  /** Polls the thread's transcript while the scope is open. */
  readonly watch: (threadId: ThreadId) => Effect.Effect<void, never, Scope.Scope>;
  /** One poll of every watched thread; a background loop runs it every few seconds. */
  readonly pollWatched: Effect.Effect<void>;
}

export class ImportedTranscriptSync extends Context.Service<
  ImportedTranscriptSync,
  ImportedTranscriptSyncShape
>()("t3/project/ImportedTranscriptSync") {}

const syncTurnItemId = (threadId: ThreadId, uuid: string) =>
  TurnItemId.make(`${SYNC_EVENT_PREFIX}:turn-item:${threadId}:${uuid}`);

/** Claude Code names a session's project directory after the cwd it runs in. */
const claudeProjectDirName = (cwd: string) => cwd.replace(/[^a-zA-Z0-9]/g, "-");

type TranscriptIdentity = ReturnType<typeof AgentSessionScanner.transcriptIdentity>;

const isImportedClaudeThreadId = (threadId: ThreadId) => threadId.startsWith("import:");

const toMs = (value: DateTime.Utc | null) =>
  value === null ? null : DateTime.toEpochMillis(value);

const concatBytes = (parts: ReadonlyArray<Uint8Array>, byteLength: number) => {
  if (parts.length === 1) return parts[0]!;
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }
  return bytes;
};

/** The offset a sync can resume from: same file, not shrunk, and still a line boundary there. */
const resumeOffset = Effect.fn("ImportedTranscriptSync.resumeOffset")(function* (
  file: FileSystem.File,
  opened: TranscriptIdentity,
  progress: AgentSessionImportSource | undefined,
) {
  const offset = progress?.syncedThroughOffset;
  if (
    progress === undefined ||
    offset === undefined ||
    offset === 0 ||
    offset > opened.size ||
    !AgentSessionScanner.sameTranscriptIdentity(
      { ...progress, size: opened.size, mtimeMs: opened.mtimeMs },
      opened,
    )
  ) {
    return 0;
  }
  yield* file.seek(BigInt(offset - 1), "start");
  const before = yield* file.readAlloc(1);
  return Option.isSome(before) && before.value[0] === 10 ? offset : 0;
});

/**
 * Streams complete lines from `start` until `size`, stopping at the first line end past
 * `maxBytes`. `endOffset` is the end of the last complete line read.
 */
const readTranscriptLines = Effect.fn("ImportedTranscriptSync.readTranscriptLines")(function* (
  file: FileSystem.File,
  input: { readonly start: number; readonly size: number; readonly maxBytes: number },
  onLine: (line: string) => void,
) {
  yield* file.seek(BigInt(input.start), "start");
  const decoder = new TextDecoder();
  let position = input.start;
  let endOffset = input.start;
  let parts: Array<Uint8Array> = [];
  let lineBytes = 0;
  while (position < input.size) {
    const chunk = yield* file.readAlloc(Math.min(READ_CHUNK_BYTES, input.size - position));
    if (Option.isNone(chunk) || chunk.value.byteLength === 0) break;
    const bytes = chunk.value;
    let from = 0;
    while (from < bytes.byteLength) {
      const newline = bytes.indexOf(10, from);
      const end = newline === -1 ? bytes.byteLength : newline;
      lineBytes += end - from;
      if (lineBytes > MAX_TRANSCRIPT_LINE_BYTES) parts = [];
      else parts.push(bytes.subarray(from, end));
      if (newline === -1) break;
      if (lineBytes <= MAX_TRANSCRIPT_LINE_BYTES) {
        onLine(decoder.decode(concatBytes(parts, lineBytes)));
      }
      parts = [];
      lineBytes = 0;
      endOffset = position + newline + 1;
      from = newline + 1;
      if (endOffset - input.start >= input.maxBytes) {
        return { endOffset, caughtUp: endOffset >= input.size };
      }
    }
    position += bytes.byteLength;
  }
  return { endOffset, caughtUp: true };
});

const make = Effect.fn("ImportedTranscriptSync.make")(function* (options?: {
  readonly maxSyncBytes?: number;
}) {
  const maxSyncBytes = options?.maxSyncBytes ?? MAX_SYNC_BYTES;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const homeDir = yield* HostProcess.HomeDirectory;
  const hostEnvironment = yield* HostProcess.Environment;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const positions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
  const syncLock = yield* Semaphore.make(1);

  const loadThread = Effect.fn("ImportedTranscriptSync.loadThread")(function* (threadId: ThreadId) {
    const records = yield* projections
      .getThreadRecords(threadId, ["runs", "providerThreads", "providerTurns", "turnItems"], {
        turnItemRunIds: [null],
      })
      .pipe(Effect.option);
    if (Option.isNone(records)) return null;
    const { thread, providerThreads } = records.value;
    if (thread.historyOrigin !== "v1_import" || thread.deletedAt !== null) return null;
    const providerThread = providerThreads.find(
      (candidate): candidate is OrchestrationV2ProviderThread =>
        candidate.driver === "claudeAgent" &&
        candidate.appThreadId === threadId &&
        candidate.ownerNodeId === null &&
        candidate.nativeThreadRef?.nativeId != null,
    );
    if (providerThread === undefined) return null;
    return { ...records.value, providerThread };
  });

  const recordedSource = Effect.fn("ImportedTranscriptSync.recordedSource")(function* (
    threadId: ThreadId,
    providerSessionId: string,
  ) {
    const runtime = yield* runtimes.getByThreadId({ threadId }).pipe(Effect.option);
    if (Option.isNone(runtime) || Option.isNone(runtime.value)) return undefined;
    const payload = decodeImportedTranscripts(runtime.value.value.runtimePayload);
    // A transcript that moved keeps its old entry; the latest recorded one comes last.
    return Option.getOrUndefined(payload)?.importedTranscripts?.findLast(
      (source) =>
        source.provider === "claudeAgent" && source.providerSessionId === providerSessionId,
    );
  });

  const identityAt = (filePath: string) =>
    fileSystem.stat(filePath).pipe(
      Effect.map((stats) =>
        stats.type === "File" ? AgentSessionScanner.transcriptIdentity(filePath, stats) : null,
      ),
      Effect.orElseSucceed(() => null),
    );

  const claudeProjectsDirs = Effect.fn("ImportedTranscriptSync.claudeProjectsDirs")(function* (
    recorded: AgentSessionImportSource,
  ) {
    const settings = yield* serverSettings.getSettings.pipe(Effect.option);
    const instance = Option.isNone(settings)
      ? undefined
      : (settings.value.providerInstances[recorded.providerInstanceId] ??
        (recorded.providerInstanceId === "claudeAgent"
          ? { driver: ProviderDriverKind.make("claudeAgent") }
          : undefined));
    const home =
      instance?.driver === "claudeAgent"
        ? AgentSessionScanner.claudeInstanceHome({ instance, hostEnvironment, homeDir, path })
        : null;
    // The import found the transcript under the home configured at that time.
    const recordedDir = path.dirname(path.dirname(recorded.filePath));
    return Array.from(
      new Set([
        ...(home === null ? [] : [path.join(home, "projects")]),
        ...(path.basename(recordedDir) === "projects" ? [recordedDir] : []),
      ]),
    );
  });

  /** Where Claude runs this thread's turns, as RuntimePolicy resolves it. */
  const threadCwd = Effect.fn("ImportedTranscriptSync.threadCwd")(function* (
    thread: OrchestrationV2AppThread,
  ) {
    if (thread.worktreePath !== null) return thread.worktreePath;
    const project = yield* projects.get(thread.projectId).pipe(Effect.orElseSucceed(Option.none));
    return Option.isSome(project) ? project.value.workspaceRoot : null;
  });

  const missingWarned = new Set<ThreadId>();

  /**
   * The transcript Claude appends to now: the one under the thread's cwd, else the recorded
   * one, else (when `search` allows) the newest copy of the session in any Claude project dir.
   */
  const locateTranscript = Effect.fn("ImportedTranscriptSync.locateTranscript")(function* (input: {
    readonly threadId: ThreadId;
    readonly thread: OrchestrationV2AppThread;
    readonly recorded: AgentSessionImportSource;
    readonly hint?: string | undefined;
    readonly search: boolean;
  }) {
    const fileName = `${input.recorded.providerSessionId}.jsonl`;
    const projectsDirs = yield* claudeProjectsDirs(input.recorded);
    const cwd = yield* threadCwd(input.thread);
    const tried = Array.from(
      new Set([
        ...(cwd === null
          ? []
          : projectsDirs.map((dir) => path.join(dir, claudeProjectDirName(cwd), fileName))),
        input.recorded.filePath,
        ...(input.hint === undefined ? [] : [input.hint]),
      ]),
    );
    let found: TranscriptIdentity | null = null;
    for (const filePath of tried) {
      found = yield* identityAt(filePath);
      if (found !== null) break;
    }
    const searched = found === null && input.search;
    if (searched) {
      for (const dir of projectsDirs) {
        tried.push(path.join(dir, "*", fileName));
        const entries = yield* fileSystem
          .readDirectory(dir)
          .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
        for (const entry of entries) {
          const candidate = yield* identityAt(path.join(dir, entry, fileName));
          if (
            candidate !== null &&
            (found === null || (candidate.mtimeMs ?? 0) > (found.mtimeMs ?? 0))
          ) {
            found = candidate;
          }
        }
      }
    }
    if (found !== null) {
      missingWarned.delete(input.threadId);
    } else if (searched && !missingWarned.has(input.threadId)) {
      missingWarned.add(input.threadId);
      yield* Effect.logWarning("Imported Claude transcript not found", {
        threadId: input.threadId,
        sessionId: input.recorded.providerSessionId,
        tried,
      });
    }
    return { identity: found, searched };
  });

  const isBusy = (records: {
    readonly runs: ReadonlyArray<{ readonly status: string }>;
    readonly providerThread: OrchestrationV2ProviderThread;
  }) =>
    records.runs.some((run) => WORKING_RUN_STATUSES.has(run.status)) ||
    (records.providerThread.pendingBackgroundTasks?.length ?? 0) > 0;

  /**
   * Positions place a run's items in its own band and runless items before every run. A message
   * from another client takes the band of the T3 run before it, so it sorts after that run's
   * items and before the next run's.
   */
  const reservePositions = Effect.fn("ImportedTranscriptSync.reservePositions")(function* (
    threadId: ThreadId,
    runs: ReadonlyArray<OrchestrationV2Run>,
    messages: ReadonlyArray<{ readonly uuid: string; readonly createdAt: string }>,
  ) {
    const started = runs.map((run) => ({
      ordinal: run.ordinal,
      startMs: DateTime.toEpochMillis(run.startedAt ?? run.requestedAt),
    }));
    for (const message of messages) {
      const atMs = Date.parse(message.createdAt);
      const runOrdinal = started.reduce<number | undefined>(
        (latest, run) =>
          run.startMs <= atMs && (latest === undefined || run.ordinal > latest)
            ? run.ordinal
            : latest,
        undefined,
      );
      yield* positions.allocate({
        threadId,
        turnItemId: syncTurnItemId(threadId, message.uuid),
        runId: null,
        ...(runOrdinal === undefined ? {} : { runOrdinal }),
      });
    }
  }, Effect.orDie);

  const syncUnlocked = Effect.fn("ImportedTranscriptSync.syncThread")(function* (
    threadId: ThreadId,
    options?: { readonly source?: AgentSessionImportSource },
  ) {
    if (!isImportedClaudeThreadId(threadId)) return { _tag: "NotSyncable" } as const;
    const records = yield* loadThread(threadId);
    if (records === null) return { _tag: "NotSyncable" } as const;
    const nativeSessionId = records.providerThread.nativeThreadRef!.nativeId!;
    const stored = yield* recordedSource(threadId, nativeSessionId);
    const recorded = options?.source ?? stored;
    if (recorded === undefined || recorded.provider !== "claudeAgent") {
      return { _tag: "NotSyncable" } as const;
    }
    if (isBusy(records)) return { _tag: "Busy" } as const;

    const identity =
      options?.source === undefined
        ? (yield* locateTranscript({ threadId, thread: records.thread, recorded, search: true }))
            .identity
        : yield* identityAt(recorded.filePath);
    if (identity === null) return { _tag: "Unchanged", source: recorded } as const;
    if (
      options?.source === undefined &&
      AgentSessionScanner.sameTranscriptIdentity(recorded, identity)
    ) {
      return { _tag: "Unchanged", source: recorded } as const;
    }

    // Runless items written by the import mark how far its history reached.
    const importedThroughMs = records.turnItems.reduce(
      (latest, item) =>
        item.id.startsWith(`${IMPORT_EVENT_PREFIX}:turn-item:`)
          ? Math.max(latest, DateTime.toEpochMillis(item.startedAt ?? item.updatedAt))
          : latest,
      Number.NEGATIVE_INFINITY,
    );
    const claudeProviderThreadIds = new Set(
      records.providerThreads
        .filter((thread) => thread.driver === "claudeAgent" && thread.appThreadId === threadId)
        .map((thread) => thread.id),
    );
    const t3Turns = records.providerTurns.filter((turn) =>
      claudeProviderThreadIds.has(turn.providerThreadId),
    );
    const evidence: T3TurnEvidence = {
      nativeIds: new Set(
        t3Turns.flatMap((turn) =>
          turn.nativeTurnRef?.nativeId == null || turn.nativeTurnRef.nativeId.startsWith("turn:")
            ? []
            : [turn.nativeTurnRef.nativeId],
        ),
      ),
      windows: t3Turns.flatMap((turn) => {
        const startMs = toMs(turn.startedAt);
        return startMs === null ? [] : [{ startMs, endMs: toMs(turn.completedAt) ?? startMs }];
      }),
    };
    const scanned = yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fileSystem.open(identity.filePath, { flag: "r" });
        const opened = AgentSessionScanner.transcriptIdentity(identity.filePath, yield* file.stat);
        // A re-import passes a freshly scanned source; the stored one still knows the offset.
        const progress = recorded.syncedThroughOffset === undefined ? stored : recorded;
        const start = yield* resumeOffset(file, opened, progress);
        const collector = externalMessageCollector({
          evidence,
          importedThroughMs,
          openOwnedByT3: start > 0 && progress?.syncedSegmentOwnedByT3 === true,
        });
        const read = yield* readTranscriptLines(
          file,
          { start, size: opened.size, maxBytes: maxSyncBytes },
          (line) => {
            const entry = parseClaudeTranscriptLine(line);
            if (entry !== null) collector.add(entry);
          },
        );
        return { opened, ...read, ...collector.finish() };
      }),
    ).pipe(Effect.option);
    if (Option.isNone(scanned)) return { _tag: "Unchanged", source: recorded } as const;
    const { opened, endOffset, caughtUp, messages, openOwnedByT3 } = scanned.value;
    const source: AgentSessionImportSource = {
      ...recorded,
      ...opened,
      // Short of the end, the recorded size is how far the sync got, so polls keep reading.
      size: caughtUp ? opened.size : endOffset,
      syncedThroughOffset: endOffset,
      syncedSegmentOwnedByT3: openOwnedByT3,
    };
    const existingItemIds = new Set<string>(records.turnItems.map((item) => item.id));
    const pending = messages.filter(
      (message) => !existingItemIds.has(syncTurnItemId(threadId, message.uuid)),
    );

    let appendedCount = 0;
    if (pending.length > 0) {
      yield* reservePositions(threadId, records.runs, pending);
      // A T3 run that started while the transcript was read owns records this read may have seen.
      const latest = yield* loadThread(threadId);
      if (latest === null) return { _tag: "NotSyncable" } as const;
      if (isBusy(latest) || latest.runs.length !== records.runs.length) {
        return { _tag: "Busy" } as const;
      }
      const committed = yield* eventSink
        .write({
          events: pending.flatMap((message) =>
            importedMessageEvents({
              threadId,
              prefix: SYNC_EVENT_PREFIX,
              messageKey: `sync:${message.uuid}`,
              itemKey: message.uuid,
              ordinal: 0,
              message,
            }),
          ),
        })
        .pipe(Effect.orDie);
      appendedCount = committed.length === 0 ? 0 : pending.length;
    }
    yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.orDie);
    if (appendedCount > 0) {
      // A warm Claude process has not seen the other client's turns; the next turn resumes from disk.
      const providerSessionId = records.providerThread.providerSessionId;
      if (providerSessionId !== null && (yield* sessions.isLive(providerSessionId))) {
        yield* sessions
          .releaseUnlessBusy({
            providerSessionId,
            ownProviderThreadIds: [records.providerThread.id],
            reason: "idle_timeout",
            detail: "Imported transcript changed outside T3.",
          })
          .pipe(Effect.ignore);
      }
    }
    return { _tag: "Synced", appendedCount, source, caughtUp } as const;
  });

  const syncThread: ImportedTranscriptSyncShape["syncThread"] = (threadId, options) =>
    syncLock
      .withPermits(1)(syncUnlocked(threadId, options))
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not sync an imported transcript", { threadId, cause }).pipe(
            Effect.as({ _tag: "Busy" } as const),
          ),
        ),
      );

  interface WatchEntry {
    readonly refs: number;
    /** undefined until loaded; null when the thread cannot sync. */
    readonly recorded: AgentSessionImportSource | null | undefined;
    readonly lastSeen: TranscriptIdentity | null;
    readonly pollsSinceSearch: number;
  }
  const watched = yield* Ref.make(new Map<ThreadId, WatchEntry>());

  const setEntry = (threadId: ThreadId, update: (entry: WatchEntry) => WatchEntry) =>
    Ref.update(watched, (current) => {
      const entry = current.get(threadId);
      if (entry === undefined) return current;
      return new Map(current).set(threadId, update(entry));
    });

  const pollThread = Effect.fn("ImportedTranscriptSync.pollThread")(function* (
    threadId: ThreadId,
    entry: WatchEntry,
  ) {
    if (entry.recorded === null) return;
    if (entry.recorded === undefined) {
      // The first poll syncs unconditionally so an opened thread catches up at once.
      const result = yield* syncThread(threadId);
      yield* setEntry(threadId, (current) => ({
        ...current,
        recorded:
          result._tag === "NotSyncable" ? null : result._tag === "Busy" ? undefined : result.source,
      }));
      return;
    }
    // The thread's worktree can change while it is open, and the transcript with it.
    const thread = yield* projections.getThread(threadId).pipe(Effect.option);
    if (Option.isNone(thread)) return;
    const located = yield* locateTranscript({
      threadId,
      thread: thread.value,
      recorded: entry.recorded,
      hint: entry.lastSeen?.filePath,
      search: entry.pollsSinceSearch >= TRANSCRIPT_SEARCH_EVERY_POLLS,
    });
    yield* setEntry(threadId, (current) => ({
      ...current,
      pollsSinceSearch: located.searched ? 0 : current.pollsSinceSearch + 1,
    }));
    const identity = located.identity;
    if (identity === null) return;
    if (AgentSessionScanner.sameTranscriptIdentity(entry.recorded, identity)) return;
    // Wait for one quiet interval so a turn still streaming is read once, after it settles.
    if (
      entry.lastSeen === null ||
      !AgentSessionScanner.sameTranscriptIdentity(entry.lastSeen, identity)
    ) {
      yield* setEntry(threadId, (current) => ({ ...current, lastSeen: identity }));
      return;
    }
    const result = yield* syncThread(threadId);
    if (result._tag === "Busy") return;
    yield* setEntry(threadId, (current) => ({
      ...current,
      recorded: result._tag === "NotSyncable" ? null : result.source,
      // A catch-up cut short by the budget continues on the next poll.
      lastSeen: result._tag === "Synced" && !result.caughtUp ? identity : null,
    }));
  });

  const pollWatched = Ref.get(watched).pipe(
    Effect.flatMap((current) =>
      Effect.forEach(current, ([threadId, entry]) => pollThread(threadId, entry), {
        discard: true,
      }),
    ),
    Effect.catchCause((cause) => Effect.logWarning("Imported transcript poll failed", { cause })),
  );
  yield* forkParked(pollWatched.pipe(Effect.repeat(Schedule.spaced(TRANSCRIPT_POLL_INTERVAL))));

  const watch: ImportedTranscriptSyncShape["watch"] = (threadId) =>
    isImportedClaudeThreadId(threadId)
      ? Effect.acquireRelease(
          Ref.modify(watched, (current) => {
            const entry = current.get(threadId);
            if (entry === undefined && current.size >= MAX_WATCHED_THREADS) {
              return [false, current] as const;
            }
            return [
              true,
              new Map(current).set(threadId, {
                refs: (entry?.refs ?? 0) + 1,
                recorded: entry?.recorded,
                lastSeen: entry?.lastSeen ?? null,
                // The first sync already searched.
                pollsSinceSearch: entry?.pollsSinceSearch ?? 0,
              }),
            ] as const;
          }),
          (added) =>
            added
              ? Ref.update(watched, (current) => {
                  const entry = current.get(threadId);
                  if (entry === undefined) return current;
                  const updated = new Map(current);
                  if (entry.refs <= 1) updated.delete(threadId);
                  else updated.set(threadId, { ...entry, refs: entry.refs - 1 });
                  return updated;
                })
              : Effect.void,
        ).pipe(Effect.asVoid)
      : Effect.void;

  return ImportedTranscriptSync.of({ syncThread, watch, pollWatched });
});

export const layerWith = (options: { readonly maxSyncBytes?: number }) =>
  Layer.effect(ImportedTranscriptSync, make(options));

export const layer = Layer.effect(ImportedTranscriptSync, make());
