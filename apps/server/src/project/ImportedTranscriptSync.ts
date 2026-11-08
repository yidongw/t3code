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
  type OrchestrationV2ProviderTurn,
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
import * as Queue from "effect/Queue";
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
// Imported threads nobody has open are checked by a sweep: one stat per thread while unchanged.
const IMPORTED_SWEEP_STARTUP_DELAY = Duration.seconds(10);
// As often as an open thread is polled, so an unopened thread is no more than a few seconds behind.
const IMPORTED_SWEEP_INTERVAL = TRANSCRIPT_POLL_INTERVAL;
/** How many sweeps, at `interval`, make up `period` (at least one). */
export const sweepsPerPeriod = (period: Duration.Duration, interval: Duration.Duration): number =>
  Math.max(1, Math.round(Duration.toMillis(period) / Duration.toMillis(interval)));
// A swept thread's cwd, archive state and transcript path are re-resolved every 5 minutes.
const IMPORTED_SWEEP_RELOCATE_EVERY = sweepsPerPeriod(Duration.minutes(5), IMPORTED_SWEEP_INTERVAL);
// A missing transcript is searched for in every project directory about once a minute.
const IMPORTED_SWEEP_SEARCH_EVERY = sweepsPerPeriod(Duration.minutes(1), IMPORTED_SWEEP_INTERVAL);
// Claude stamps a prompt after T3 marks the turn started and can flush records just after it settles.
const T3_TURN_LEAD_MS = 2_000;
const T3_TURN_GRACE_MS = 5_000;
const WORKING_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "starting",
  "running",
  "waiting",
]);

const BACKFILL_EVENT_PREFIX = "agent-session-backfill:v1";
// A backfill step reads at most this much and adds at most about this many messages.
const MAX_BACKFILL_STEP_BYTES = 8 * 1024 * 1024;
const MAX_BACKFILL_STEP_MESSAGES = 200;
const BACKFILL_STEP_PAUSE = Duration.millis(250);
// Imported threads that a T3 run or a missing transcript held back are retried by later sweeps.
const BACKFILL_SWEEP_DELAY = Duration.seconds(30);
const BACKFILL_SWEEP_INTERVAL = Duration.minutes(10);
const COMPLETE_HISTORY_NOTICE =
  "Earlier history was imported from the original session transcript, which is unchanged.";

/**
 * Progress of copying an imported thread's full history from its transcript, kept in the
 * thread's runtime payload. `plan` numbers the transcript's messages up to the end of the
 * import, `relayout` moves the imported items to their numbers past `base`, and `fill` adds the
 * missing messages at theirs. `offset` is the end of the last line a phase read.
 */
const ImportedHistoryBackfill = Schema.Struct({
  phase: Schema.Literals(["plan", "relayout", "fill", "done", "skipped"]),
  filePath: Schema.String,
  device: Schema.Number,
  inode: Schema.NullOr(Schema.Union([Schema.String, Schema.Number])),
  birthtimeMs: Schema.NullOr(Schema.Number),
  offset: Schema.Number,
  segmentOwnedByT3: Schema.Boolean,
  messageCount: Schema.Number,
  matchedCount: Schema.Number,
  /** The number of each imported message, in thread order. */
  importedNumbers: Schema.Array(Schema.Number),
  plannedCount: Schema.optional(Schema.Number),
  base: Schema.optional(Schema.Number),
  addedCount: Schema.Number,
  reason: Schema.optional(Schema.String),
});
type ImportedHistoryBackfill = typeof ImportedHistoryBackfill.Type;

const decodeImportedTranscripts = Schema.decodeUnknownOption(
  Schema.Struct({ importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)) }),
);
const decodeImportedHistoryBackfill = Schema.decodeUnknownOption(
  Schema.Struct({ importedHistoryBackfill: ImportedHistoryBackfill }),
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

function t3TurnEvidence(
  threadId: ThreadId,
  records: {
    readonly providerThreads: ReadonlyArray<OrchestrationV2ProviderThread>;
    readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  },
): T3TurnEvidence {
  const claudeProviderThreadIds = new Set(
    records.providerThreads
      .filter((thread) => thread.driver === "claudeAgent" && thread.appThreadId === threadId)
      .map((thread) => thread.id),
  );
  const t3Turns = records.providerTurns.filter((turn) =>
    claudeProviderThreadIds.has(turn.providerThreadId),
  );
  return {
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
  readonly onMessage?: (message: ExternalMessage) => void;
}) {
  const messages: Array<ExternalMessage> = [];
  const emit = input.onMessage ?? ((message: ExternalMessage) => messages.push(message));
  let segment: Array<ExternalMessage> = [];
  let ownedByT3 = input.openOwnedByT3;
  const close = () => {
    if (!ownedByT3) segment.forEach(emit);
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
  /**
   * One bounded step of copying the history an import left out from the transcript. A
   * background worker runs steps for threads that are opened, imported or found by its sweep.
   */
  readonly backfillStep: (threadId: ThreadId) => Effect.Effect<ImportedHistoryBackfillStep>;
  /** Queues the thread's backfill ahead of the startup sweep. */
  readonly requestBackfill: (threadId: ThreadId) => Effect.Effect<void>;
  /**
   * One pass over every imported Claude thread that is not open, syncing those whose transcript
   * changed. A background loop runs it at startup and every half minute.
   */
  readonly sweepImported: Effect.Effect<{
    readonly threadsSynced: number;
    readonly messagesAppended: number;
  }>;
}

export class ImportedTranscriptSync extends Context.Service<
  ImportedTranscriptSync,
  ImportedTranscriptSyncShape
>()("t3/project/ImportedTranscriptSync") {}

export type ImportedHistoryBackfillStep =
  | { readonly _tag: "NotSyncable" }
  | { readonly _tag: "Busy" }
  | { readonly _tag: "TranscriptMissing" }
  | {
      readonly _tag: "Progress";
      readonly phase: "plan" | "relayout" | "fill";
      readonly addedCount: number;
    }
  | { readonly _tag: "Done"; readonly addedCount: number }
  | { readonly _tag: "Skipped"; readonly reason: string };

const syncTurnItemId = (threadId: ThreadId, uuid: string) =>
  TurnItemId.make(`${SYNC_EVENT_PREFIX}:turn-item:${threadId}:${uuid}`);
const backfillTurnItemId = (threadId: ThreadId, uuid: string) =>
  TurnItemId.make(`${BACKFILL_EVENT_PREFIX}:turn-item:${threadId}:${uuid}`);

/** The notice an import writes when it keeps only part of the history. */
export const historyNoticeItemId = (threadId: ThreadId) =>
  TurnItemId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`);

type ConversationItem = Extract<
  OrchestrationV2TurnItem,
  { readonly type: "user_message" | "assistant_message" }
>;

const isConversationItem = (item: OrchestrationV2TurnItem): item is ConversationItem =>
  item.type === "user_message" || item.type === "assistant_message";

const itemStartedMs = (item: OrchestrationV2TurnItem) =>
  DateTime.toEpochMillis(item.startedAt ?? item.updatedAt);

/** The importer shaped its messages as the transcript does, so role, time and text match. */
const sameConversationMessage = (
  item: ConversationItem,
  message: AgentSessionScanner.AgentSessionThreadMessage,
) =>
  (item.type === "user_message" ? "user" : "assistant") === message.role &&
  item.text === message.text &&
  itemStartedMs(item) === Date.parse(message.createdAt);

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

/** The offset a read can resume from: same file, not shrunk, and still a line boundary there. */
const resumeOffset = Effect.fn("ImportedTranscriptSync.resumeOffset")(function* (
  file: FileSystem.File,
  opened: TranscriptIdentity,
  progress:
    | (Pick<AgentSessionImportSource, "filePath" | "device" | "inode" | "birthtimeMs"> & {
        readonly offset: number | undefined;
      })
    | undefined,
) {
  const offset = progress?.offset;
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
 * `maxBytes` or after a line `onLine` returns false for. `endOffset` is the end of the last
 * complete line read.
 */
const readTranscriptLines = Effect.fn("ImportedTranscriptSync.readTranscriptLines")(function* (
  file: FileSystem.File,
  input: { readonly start: number; readonly size: number; readonly maxBytes: number },
  onLine: (line: string) => boolean | void,
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
      const more =
        lineBytes > MAX_TRANSCRIPT_LINE_BYTES ||
        onLine(decoder.decode(concatBytes(parts, lineBytes))) !== false;
      parts = [];
      lineBytes = 0;
      endOffset = position + newline + 1;
      from = newline + 1;
      if (!more || endOffset - input.start >= input.maxBytes) {
        return { endOffset, caughtUp: endOffset >= input.size };
      }
    }
    position += bytes.byteLength;
  }
  return { endOffset, caughtUp: true };
});

interface ImportedTranscriptSyncOptions {
  readonly maxSyncBytes?: number;
  readonly maxBackfillStepBytes?: number;
  readonly maxBackfillStepMessages?: number;
}

const make = Effect.fn("ImportedTranscriptSync.make")(function* (
  options?: ImportedTranscriptSyncOptions,
) {
  const maxSyncBytes = options?.maxSyncBytes ?? MAX_SYNC_BYTES;
  const maxBackfillStepBytes = options?.maxBackfillStepBytes ?? MAX_BACKFILL_STEP_BYTES;
  const maxBackfillStepMessages = options?.maxBackfillStepMessages ?? MAX_BACKFILL_STEP_MESSAGES;
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
    const evidence = t3TurnEvidence(threadId, records);
    const scanned = yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fileSystem.open(identity.filePath, { flag: "r" });
        const opened = AgentSessionScanner.transcriptIdentity(identity.filePath, yield* file.stat);
        // A re-import passes a freshly scanned source; the stored one still knows the offset.
        const progress = recorded.syncedThroughOffset === undefined ? stored : recorded;
        const start = yield* resumeOffset(
          file,
          opened,
          progress === undefined
            ? undefined
            : { ...progress, offset: progress.syncedThroughOffset },
        );
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

  const recordedBackfill = Effect.fn("ImportedTranscriptSync.recordedBackfill")(function* (
    threadId: ThreadId,
  ) {
    const runtime = yield* runtimes.getByThreadId({ threadId }).pipe(Effect.option);
    if (Option.isNone(runtime) || Option.isNone(runtime.value)) return undefined;
    return Option.getOrUndefined(decodeImportedHistoryBackfill(runtime.value.value.runtimePayload))
      ?.importedHistoryBackfill;
  });

  const saveBackfill = (threadId: ThreadId, state: ImportedHistoryBackfill) =>
    runtimes.recordImportedHistoryBackfill({ threadId, state }).pipe(Effect.orDie);

  /** Whether T3 is running the thread, read without its turn items. */
  const runState = Effect.fn("ImportedTranscriptSync.runState")(function* (threadId: ThreadId) {
    const records = yield* projections
      .getThreadRecords(threadId, ["runs", "providerThreads"])
      .pipe(Effect.option);
    if (Option.isNone(records)) return null;
    const { runs, providerThreads } = records.value;
    return {
      busy:
        runs.some((run) => WORKING_RUN_STATUSES.has(run.status)) ||
        providerThreads.some(
          (thread) =>
            thread.appThreadId === threadId && (thread.pendingBackgroundTasks?.length ?? 0) > 0,
        ),
      runCount: runs.length,
    };
  });

  /** A T3 run that started since `runCount` runs existed owns what the step read. */
  const runStartedSince = Effect.fn("ImportedTranscriptSync.runStartedSince")(function* (
    threadId: ThreadId,
    runCount: number,
  ) {
    const latest = yield* runState(threadId);
    return latest === null || latest.busy || latest.runCount !== runCount;
  });

  const historyNoticeEvent = Effect.fn("ImportedTranscriptSync.historyNoticeEvent")(function* (
    threadId: ThreadId,
    notice: OrchestrationV2TurnItem | undefined,
  ) {
    if (notice?.type !== "system_notice" || notice.message === COMPLETE_HISTORY_NOTICE) {
      return [];
    }
    const now = yield* DateTime.now;
    return [
      {
        id: EventId.make(`${BACKFILL_EVENT_PREFIX}:history-notice:${threadId}`),
        type: "turn-item.updated" as const,
        threadId,
        occurredAt: now,
        payload: { ...notice, message: COMPLETE_HISTORY_NOTICE, updatedAt: now },
      },
    ];
  });

  /**
   * One bounded step of copying the transcript's messages from before the end of the import into
   * the thread. Positions are integers, so the imported items, which sit next to each other, move
   * once to the numbers the transcript gives them, leaving a free position for every missing one.
   */
  const backfillUnlocked = Effect.fn("ImportedTranscriptSync.backfillStep")(function* (
    threadId: ThreadId,
  ) {
    if (!isImportedClaudeThreadId(threadId)) return { _tag: "NotSyncable" } as const;
    const saved = yield* recordedBackfill(threadId);
    if (saved?.phase === "done") return { _tag: "Done", addedCount: saved.addedCount } as const;
    if (saved?.phase === "skipped") {
      return { _tag: "Skipped", reason: saved.reason ?? "unknown" } as const;
    }
    const running = yield* runState(threadId);
    if (running === null) return { _tag: "NotSyncable" } as const;
    if (running.busy) return { _tag: "Busy" } as const;
    const records = yield* loadThread(threadId);
    if (records === null) return { _tag: "NotSyncable" } as const;
    const recorded = yield* recordedSource(
      threadId,
      records.providerThread.nativeThreadRef!.nativeId!,
    );
    if (recorded === undefined) return { _tag: "NotSyncable" } as const;
    if (isBusy(records)) return { _tag: "Busy" } as const;

    const importedItems = records.turnItems
      .filter(
        (item): item is ConversationItem =>
          isConversationItem(item) && item.id.startsWith(`${IMPORT_EVENT_PREFIX}:turn-item:`),
      )
      .toSorted((left, right) => left.ordinal - right.ordinal);
    const importedThroughMs = Math.max(...importedItems.map(itemStartedMs));
    const notice = records.turnItems.find((item) => item.id === historyNoticeItemId(threadId));
    const existingIds = new Set<string>(records.turnItems.map((item) => item.id));

    const located = yield* locateTranscript({
      threadId,
      thread: records.thread,
      recorded,
      search: false,
    });
    if (located.identity === null) return { _tag: "TranscriptMissing" } as const;
    const identity = located.identity;

    return yield* Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fileSystem.open(identity.filePath, { flag: "r" });
        const opened = AgentSessionScanner.transcriptIdentity(identity.filePath, yield* file.stat);
        const fileFields = {
          filePath: opened.filePath,
          device: opened.device,
          inode: opened.inode,
          birthtimeMs: opened.birthtimeMs,
        };
        const fresh: ImportedHistoryBackfill = {
          phase: "plan",
          ...fileFields,
          offset: 0,
          segmentOwnedByT3: false,
          messageCount: 0,
          matchedCount: 0,
          importedNumbers: [],
          addedCount: 0,
        };
        let state = saved ?? fresh;
        const skip = Effect.fn("ImportedTranscriptSync.skipBackfill")(function* (reason: string) {
          yield* Effect.logWarning("Imported Claude history was not backfilled", {
            threadId,
            reason,
            filePath: opened.filePath,
          });
          yield* saveBackfill(threadId, { ...state, phase: "skipped", reason });
          return { _tag: "Skipped", reason } as const;
        });
        const finish = Effect.fn("ImportedTranscriptSync.finishBackfill")(function* (
          addedCount: number,
        ) {
          const events = yield* historyNoticeEvent(threadId, notice);
          if (events.length > 0) yield* eventSink.write({ events }).pipe(Effect.orDie);
          yield* saveBackfill(threadId, { ...state, phase: "done", addedCount });
          return { _tag: "Done", addedCount } as const;
        });

        if (importedItems.length === 0) return yield* skip("no-imported-messages");
        // Imports come before T3 runs, so all of this history belongs before the first run.
        if (
          records.runs.some(
            (run) => DateTime.toEpochMillis(run.startedAt ?? run.requestedAt) <= importedThroughMs,
          )
        ) {
          return yield* skip("run-before-import-end");
        }

        const start = yield* resumeOffset(file, opened, state);
        if (start !== state.offset) {
          // Another file, or this one rewritten: a phase reads its file from the start again.
          state =
            state.phase === "plan"
              ? fresh
              : {
                  ...state,
                  ...fileFields,
                  offset: 0,
                  segmentOwnedByT3: false,
                  messageCount: 0,
                  matchedCount: 0,
                };
        } else {
          state = { ...state, ...fileFields };
        }

        if (state.phase === "plan") {
          let messageCount = state.messageCount;
          let matchedCount = state.matchedCount;
          const importedNumbers = [...state.importedNumbers];
          let reachedEnd = false;
          const collector = externalMessageCollector({
            evidence: t3TurnEvidence(threadId, records),
            importedThroughMs: Number.NEGATIVE_INFINITY,
            openOwnedByT3: state.segmentOwnedByT3,
            onMessage: (message) => {
              if (reachedEnd) return;
              if (Date.parse(message.createdAt) > importedThroughMs) {
                reachedEnd = true;
                return;
              }
              const next = importedItems[matchedCount];
              if (next !== undefined && sameConversationMessage(next, message)) {
                importedNumbers.push(messageCount);
                matchedCount += 1;
              }
              messageCount += 1;
            },
          });
          const read = yield* readTranscriptLines(
            file,
            { start: state.offset, size: opened.size, maxBytes: maxBackfillStepBytes },
            (line) => {
              const entry = parseClaudeTranscriptLine(line);
              if (entry !== null) collector.add(entry);
              return !reachedEnd;
            },
          );
          const { openOwnedByT3 } = collector.finish();
          state = {
            ...state,
            offset: read.endOffset,
            segmentOwnedByT3: openOwnedByT3,
            messageCount,
            matchedCount,
            importedNumbers,
          };
          if (!reachedEnd && !read.caughtUp) {
            yield* saveBackfill(threadId, state);
            return { _tag: "Progress", phase: "plan", addedCount: 0 } as const;
          }
          if (matchedCount < importedItems.length) return yield* skip("transcript-mismatch");
          if (messageCount === matchedCount) return yield* finish(0);
          const synced = records.turnItems.filter(
            (item) =>
              item.runId === null &&
              item.ordinal < TurnItemPositionStore.RUN_BAND_SIZE &&
              item.id.startsWith(`${SYNC_EVENT_PREFIX}:turn-item:`),
          );
          const base =
            ((yield* positions.maxInBand({ threadId, runOrdinal: 0 }).pipe(Effect.orDie)) ?? 0) + 1;
          if (base + messageCount + synced.length >= TurnItemPositionStore.RUN_BAND_SIZE) {
            return yield* skip("history-too-long");
          }
          state = {
            ...state,
            phase: "relayout",
            base,
            plannedCount: messageCount,
            offset: 0,
            segmentOwnedByT3: false,
            messageCount: 0,
            matchedCount: 0,
          };
          yield* saveBackfill(threadId, state);
        }

        const base = state.base!;
        const plannedCount = state.plannedCount!;

        if (state.phase === "relayout") {
          // Synced items came after the import, in transcript order; they follow the planned ones.
          const synced = records.turnItems
            .filter(
              (item) =>
                item.runId === null &&
                item.ordinal < TurnItemPositionStore.RUN_BAND_SIZE &&
                item.id.startsWith(`${SYNC_EVENT_PREFIX}:turn-item:`),
            )
            .toSorted((left, right) => left.ordinal - right.ordinal);
          const targets = [
            ...importedItems.map((item, index) => ({
              item,
              ordinal: base + state.importedNumbers[index]!,
            })),
            ...synced.map((item, index) => ({ item, ordinal: base + plannedCount + index })),
          ];
          yield* positions
            .place({
              threadId,
              positions: targets.map(({ item, ordinal }) => ({ turnItemId: item.id, ordinal })),
            })
            .pipe(Effect.orDie);
          // Newest first: a client showing only recent history moves the rows it holds, and
          // takes the rest for older history it has not loaded.
          const moved = targets
            .filter(({ item, ordinal }) => item.ordinal !== ordinal)
            .toSorted((left, right) => right.ordinal - left.ordinal);
          if (moved.length > 0) {
            if (yield* runStartedSince(threadId, records.runs.length)) {
              return { _tag: "Busy" } as const;
            }
            const now = yield* DateTime.now;
            yield* eventSink
              .write({
                events: moved.map(({ item, ordinal }) => ({
                  id: EventId.make(
                    `${BACKFILL_EVENT_PREFIX}:position:${threadId}:${base}:${item.id}`,
                  ),
                  type: "turn-item.updated" as const,
                  threadId,
                  occurredAt: now,
                  payload: { ...item, ordinal },
                })),
              })
              .pipe(Effect.orDie);
          }
          state = { ...state, phase: "fill" };
          yield* saveBackfill(threadId, state);
          return { _tag: "Progress", phase: "relayout", addedCount: 0 } as const;
        }

        let messageCount = state.messageCount;
        let matchedCount = state.matchedCount;
        let reachedEnd = false;
        let inconsistent = false;
        const pending: Array<{ readonly message: ExternalMessage; readonly ordinal: number }> = [];
        const collector = externalMessageCollector({
          evidence: t3TurnEvidence(threadId, records),
          importedThroughMs: Number.NEGATIVE_INFINITY,
          openOwnedByT3: state.segmentOwnedByT3,
          onMessage: (message) => {
            if (reachedEnd || inconsistent) return;
            if (Date.parse(message.createdAt) > importedThroughMs) {
              reachedEnd = true;
              return;
            }
            const next = importedItems[matchedCount];
            if (next !== undefined && sameConversationMessage(next, message)) {
              inconsistent ||= state.importedNumbers[matchedCount] !== messageCount;
              matchedCount += 1;
            } else if (messageCount >= plannedCount) {
              inconsistent = true;
            } else if (
              !existingIds.has(backfillTurnItemId(threadId, message.uuid)) &&
              !existingIds.has(syncTurnItemId(threadId, message.uuid))
            ) {
              pending.push({ message, ordinal: base + messageCount });
            }
            messageCount += 1;
          },
        });
        const read = yield* readTranscriptLines(
          file,
          { start: state.offset, size: opened.size, maxBytes: maxBackfillStepBytes },
          (line) => {
            const entry = parseClaudeTranscriptLine(line);
            if (entry !== null) collector.add(entry);
            return !reachedEnd && !inconsistent && pending.length < maxBackfillStepMessages;
          },
        );
        const { openOwnedByT3 } = collector.finish();
        if (inconsistent) return yield* skip("transcript-changed");
        if (pending.length > 0) {
          yield* positions
            .place({
              threadId,
              positions: pending.map(({ message, ordinal }) => ({
                turnItemId: backfillTurnItemId(threadId, message.uuid),
                ordinal,
              })),
            })
            .pipe(Effect.orDie);
          if (yield* runStartedSince(threadId, records.runs.length)) {
            return { _tag: "Busy" } as const;
          }
          yield* eventSink
            .write({
              events: pending.flatMap(({ message, ordinal }) =>
                importedMessageEvents({
                  threadId,
                  prefix: BACKFILL_EVENT_PREFIX,
                  messageKey: `backfill:${message.uuid}`,
                  itemKey: message.uuid,
                  ordinal,
                  message,
                }),
              ),
            })
            .pipe(Effect.orDie);
        }
        state = {
          ...state,
          offset: read.endOffset,
          segmentOwnedByT3: openOwnedByT3,
          messageCount,
          matchedCount,
          addedCount: state.addedCount + pending.length,
        };
        if (reachedEnd || read.caughtUp) {
          if (matchedCount < importedItems.length) return yield* skip("transcript-changed");
          return yield* finish(state.addedCount);
        }
        yield* saveBackfill(threadId, state);
        return { _tag: "Progress", phase: "fill", addedCount: pending.length } as const;
      }),
    );
  });

  const backfillStep: ImportedTranscriptSyncShape["backfillStep"] = (threadId) =>
    syncLock
      .withPermits(1)(backfillUnlocked(threadId))
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not backfill an imported thread", { threadId, cause }).pipe(
            Effect.as({ _tag: "Busy" } as const),
          ),
        ),
      );

  // Opened and newly imported threads go first; the startup sweep queues the rest behind them.
  const backfillQueue = yield* Ref.make<{
    readonly urgent: ReadonlyArray<ThreadId>;
    readonly background: ReadonlyArray<ThreadId>;
  }>({ urgent: [], background: [] });
  const backfillWake = yield* Queue.unbounded<void>();
  const backfillSettled = new Set<ThreadId>();

  const queueBackfill = (threadId: ThreadId, priority: "first" | "urgent" | "background") =>
    !isImportedClaudeThreadId(threadId) || backfillSettled.has(threadId)
      ? Effect.void
      : Ref.update(backfillQueue, ({ urgent, background }) => {
          const queued = urgent.includes(threadId) || background.includes(threadId);
          if (priority === "first") {
            return {
              urgent: [threadId, ...urgent.filter((id) => id !== threadId)],
              background: background.filter((id) => id !== threadId),
            };
          }
          if (queued) return { urgent, background };
          return priority === "urgent"
            ? { urgent: [...urgent, threadId], background }
            : { urgent, background: [...background, threadId] };
        }).pipe(Effect.andThen(Queue.offer(backfillWake, undefined)), Effect.asVoid);

  const runBackfills = Effect.gen(function* () {
    while (true) {
      const { urgent, background } = yield* Ref.get(backfillQueue);
      const threadId = urgent[0] ?? background[0];
      if (threadId === undefined) {
        yield* Queue.take(backfillWake);
        continue;
      }
      const step = yield* backfillStep(threadId);
      if (step._tag !== "Progress") {
        // Busy and missing transcripts wait for a poll of the open thread or the next sweep.
        yield* Ref.update(backfillQueue, (current) => ({
          urgent: current.urgent.filter((id) => id !== threadId),
          background: current.background.filter((id) => id !== threadId),
        }));
      }
      if (step._tag === "Done" || step._tag === "Skipped" || step._tag === "NotSyncable") {
        backfillSettled.add(threadId);
      }
      yield* Effect.sleep(BACKFILL_STEP_PAUSE);
    }
  });
  yield* forkParked(runBackfills);

  const sweepBackfills = runtimes.list().pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(
        rows,
        (row) => {
          const phase = Option.getOrUndefined(decodeImportedHistoryBackfill(row.runtimePayload))
            ?.importedHistoryBackfill.phase;
          const claude = Option.getOrUndefined(
            decodeImportedTranscripts(row.runtimePayload),
          )?.importedTranscripts?.some((source) => source.provider === "claudeAgent");
          return claude === true && phase !== "done" && phase !== "skipped"
            ? queueBackfill(row.threadId, "background")
            : Effect.void;
        },
        { discard: true },
      ),
    ),
    Effect.catchCause((cause) => Effect.logWarning("Imported history sweep failed", { cause })),
  );
  yield* forkParked(
    Effect.sleep(BACKFILL_SWEEP_DELAY).pipe(
      Effect.andThen(sweepBackfills.pipe(Effect.repeat(Schedule.spaced(BACKFILL_SWEEP_INTERVAL)))),
    ),
  );

  const requestBackfill: ImportedTranscriptSyncShape["requestBackfill"] = (threadId) =>
    queueBackfill(threadId, "urgent");

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
    // A backfill a T3 run held back resumes once the run is over.
    yield* queueBackfill(threadId, "urgent");
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
        ).pipe(Effect.andThen(queueBackfill(threadId, "first")))
      : Effect.void;

  interface SweepEntry {
    /** The transcript last located; null when none was found. */
    readonly filePath: string | null;
    /** Archived, deleted or no longer an imported Claude thread when last resolved. */
    readonly skipped: boolean;
    readonly sweepsSinceLocate: number;
    readonly sweepsSinceSearch: number;
  }
  // Only the sweep loop touches this.
  const swept = new Map<ThreadId, SweepEntry>();

  /** Appended message count, or null when the thread did not sync. */
  const sweepThread = Effect.fn("ImportedTranscriptSync.sweepThread")(function* (
    threadId: ThreadId,
    recorded: AgentSessionImportSource,
  ) {
    const entry: SweepEntry = swept.get(threadId) ?? {
      filePath: null,
      skipped: false,
      sweepsSinceLocate: IMPORTED_SWEEP_RELOCATE_EVERY,
      sweepsSinceSearch: IMPORTED_SWEEP_SEARCH_EVERY,
    };
    if (entry.sweepsSinceLocate < IMPORTED_SWEEP_RELOCATE_EVERY) {
      const next = { ...entry, sweepsSinceLocate: entry.sweepsSinceLocate + 1 };
      if (entry.skipped) {
        swept.set(threadId, next);
        return null;
      }
      if (entry.filePath !== null) {
        const identity = yield* identityAt(entry.filePath);
        if (identity !== null && AgentSessionScanner.sameTranscriptIdentity(recorded, identity)) {
          swept.set(threadId, next);
          return null;
        }
      }
    }
    // Changed, missing or due: resolve the thread before reading anything.
    const thread = yield* projections.getThread(threadId).pipe(Effect.option);
    if (
      Option.isNone(thread) ||
      thread.value.historyOrigin !== "v1_import" ||
      thread.value.archivedAt !== null ||
      thread.value.deletedAt !== null
    ) {
      swept.set(threadId, { ...entry, filePath: null, skipped: true, sweepsSinceLocate: 0 });
      return null;
    }
    const located = yield* locateTranscript({
      threadId,
      thread: thread.value,
      recorded,
      hint: entry.filePath ?? undefined,
      search: entry.sweepsSinceSearch >= IMPORTED_SWEEP_SEARCH_EVERY,
    });
    swept.set(threadId, {
      filePath: located.identity?.filePath ?? null,
      skipped: false,
      sweepsSinceLocate: 0,
      sweepsSinceSearch: located.searched ? 0 : entry.sweepsSinceSearch + 1,
    });
    if (
      located.identity === null ||
      AgentSessionScanner.sameTranscriptIdentity(recorded, located.identity)
    ) {
      return null;
    }
    // Busy threads keep their recorded identity, so a later sweep retries them.
    const running = yield* runState(threadId);
    if (running === null || running.busy) return null;
    const result = yield* syncThread(threadId);
    if (result._tag === "NotSyncable") {
      swept.set(threadId, { ...entry, filePath: null, skipped: true, sweepsSinceLocate: 0 });
    }
    return result._tag === "Synced" ? result.appendedCount : null;
  });

  const sweepImported: ImportedTranscriptSyncShape["sweepImported"] = Effect.gen(function* () {
    const rows = yield* runtimes.list();
    const open = yield* Ref.get(watched);
    let threadsSynced = 0;
    let messagesAppended = 0;
    const seen = new Set<ThreadId>();
    for (const row of rows) {
      if (!isImportedClaudeThreadId(row.threadId)) continue;
      const recorded = Option.getOrUndefined(
        decodeImportedTranscripts(row.runtimePayload),
      )?.importedTranscripts?.findLast((source) => source.provider === "claudeAgent");
      if (recorded === undefined) continue;
      seen.add(row.threadId);
      // Open threads are polled every few seconds instead.
      if (open.has(row.threadId)) continue;
      const appended = yield* sweepThread(row.threadId, recorded);
      if (appended !== null && appended > 0) {
        threadsSynced += 1;
        messagesAppended += appended;
      }
    }
    for (const threadId of swept.keys()) {
      if (!seen.has(threadId)) swept.delete(threadId);
    }
    if (threadsSynced > 0) {
      yield* Effect.logInfo("Synced imported Claude threads", { threadsSynced, messagesAppended });
    }
    return { threadsSynced, messagesAppended };
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Imported transcript sweep failed", { cause }).pipe(
        Effect.as({ threadsSynced: 0, messagesAppended: 0 }),
      ),
    ),
  );
  // The first sweep catches up threads whose transcripts changed while the server was down.
  yield* forkParked(
    Effect.sleep(IMPORTED_SWEEP_STARTUP_DELAY).pipe(
      Effect.andThen(sweepImported.pipe(Effect.repeat(Schedule.spaced(IMPORTED_SWEEP_INTERVAL)))),
    ),
  );

  return ImportedTranscriptSync.of({
    syncThread,
    watch,
    pollWatched,
    backfillStep,
    requestBackfill,
    sweepImported,
  });
});

export const layerWith = (options: ImportedTranscriptSyncOptions) =>
  Layer.effect(ImportedTranscriptSync, make(options));

export const layer = Layer.effect(ImportedTranscriptSync, make());
