import {
  AgentSessionImportSource,
  EventId,
  MessageId,
  ThreadId,
  TurnItemId,
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
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as TurnItemPositionStore from "../orchestration-v2/TurnItemPositionStore.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import { forkParked } from "../serverActivation.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

export const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const SYNC_EVENT_PREFIX = "agent-session-sync:v1";
const TRANSCRIPT_POLL_INTERVAL = Duration.seconds(3);
const MAX_WATCHED_THREADS = 32;
const MAX_SYNC_TRANSCRIPT_BYTES = 64 * 1024 * 1024;
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

/** Keeps complete lines only; a record still being written is picked up on the next sync. */
function parseClaudeTranscriptEntries(contents: string): ReadonlyArray<TranscriptEntry> {
  const complete = contents.slice(0, contents.lastIndexOf("\n") + 1);
  const entries: Array<TranscriptEntry> = [];
  for (const line of complete.split("\n")) {
    if (line.trim().length === 0) continue;
    const record = Option.getOrUndefined(decodeClaudeLine(line));
    if (record === undefined || record.isSidechain === true) continue;
    if (record.type !== "user" && record.type !== "assistant") continue;
    const atMs = record.timestamp === undefined ? NaN : Date.parse(record.timestamp);
    const message =
      record.timestamp === undefined
        ? null
        : AgentSessionScanner.claudeTranscriptMessage(record, record.timestamp);
    entries.push({
      uuid: record.uuid,
      atMs: Number.isFinite(atMs) ? atMs : null,
      message,
      // Tool results carry no text and interrupt markers continue the interrupted turn.
      isPrompt:
        message?.role === "user" &&
        record.isMeta !== true &&
        !message.text.startsWith("[Request interrupted"),
    });
  }
  return entries;
}

interface T3TurnEvidence {
  /** Claude message uuids T3 recorded for its own turns. */
  readonly nativeIds: ReadonlySet<string>;
  readonly windows: ReadonlyArray<{ readonly startMs: number; readonly endMs: number }>;
}

/**
 * Conversation messages from segments T3 did not run. A segment starts at a user prompt; it is
 * T3's if it holds a uuid T3 recorded, or its prompt falls inside one of T3's provider turns.
 */
function externalTranscriptMessages(
  entries: ReadonlyArray<TranscriptEntry>,
  evidence: T3TurnEvidence,
  importedThroughMs: number,
): ReadonlyArray<AgentSessionScanner.AgentSessionThreadMessage & { readonly uuid: string }> {
  const segments: Array<Array<TranscriptEntry>> = [[]];
  for (const entry of entries) {
    if (entry.isPrompt && segments.at(-1)!.length > 0) segments.push([]);
    segments.at(-1)!.push(entry);
  }
  return segments.flatMap((segment) => {
    const promptAtMs = segment[0]?.isPrompt === true ? segment[0].atMs : null;
    const ownedByT3 =
      segment.some((entry) => entry.uuid !== undefined && evidence.nativeIds.has(entry.uuid)) ||
      (promptAtMs !== null &&
        evidence.windows.some(
          (window) =>
            promptAtMs >= window.startMs - T3_TURN_LEAD_MS &&
            promptAtMs <= window.endMs + T3_TURN_GRACE_MS,
        ));
    if (ownedByT3) return [];
    return segment.flatMap((entry) =>
      entry.message !== null &&
      entry.uuid !== undefined &&
      entry.atMs !== null &&
      entry.atMs > importedThroughMs
        ? [{ ...entry.message, uuid: entry.uuid }]
        : [],
    );
  });
}

export type ImportedTranscriptSyncResult =
  | { readonly _tag: "NotSyncable" }
  | { readonly _tag: "Busy" }
  | { readonly _tag: "Unchanged"; readonly source: AgentSessionImportSource }
  | {
      readonly _tag: "Synced";
      readonly appendedCount: number;
      readonly source: AgentSessionImportSource;
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
}

export class ImportedTranscriptSync extends Context.Service<
  ImportedTranscriptSync,
  ImportedTranscriptSyncShape
>()("t3/project/ImportedTranscriptSync") {}

const syncTurnItemId = (threadId: ThreadId, uuid: string) =>
  TurnItemId.make(`${SYNC_EVENT_PREFIX}:turn-item:${threadId}:${uuid}`);

const isImportedClaudeThreadId = (threadId: ThreadId) => threadId.startsWith("import:");

const toMs = (value: DateTime.Utc | null) =>
  value === null ? null : DateTime.toEpochMillis(value);

const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
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
    return Option.getOrUndefined(payload)?.importedTranscripts?.find(
      (source) =>
        source.provider === "claudeAgent" && source.providerSessionId === providerSessionId,
    );
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
    const recorded = options?.source ?? (yield* recordedSource(threadId, nativeSessionId));
    if (recorded === undefined || recorded.provider !== "claudeAgent") {
      return { _tag: "NotSyncable" } as const;
    }
    if (isBusy(records)) return { _tag: "Busy" } as const;

    const stats = yield* fileSystem.stat(recorded.filePath).pipe(Effect.option);
    if (Option.isNone(stats) || stats.value.type !== "File") {
      return { _tag: "Unchanged", source: recorded } as const;
    }
    const source: AgentSessionImportSource = {
      ...recorded,
      ...AgentSessionScanner.transcriptIdentity(recorded.filePath, stats.value),
    };
    if (
      options?.source === undefined &&
      AgentSessionScanner.sameTranscriptIdentity(recorded, source)
    ) {
      return { _tag: "Unchanged", source: recorded } as const;
    }
    if (source.size > MAX_SYNC_TRANSCRIPT_BYTES) {
      yield* Effect.logWarning("Imported transcript is too large to sync", {
        threadId,
        filePath: source.filePath,
        size: source.size,
      });
      return { _tag: "Unchanged", source: recorded } as const;
    }
    const contents = yield* fileSystem.readFileString(source.filePath).pipe(Effect.option);
    if (Option.isNone(contents)) return { _tag: "Unchanged", source: recorded } as const;

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
    const existingItemIds = new Set<string>(records.turnItems.map((item) => item.id));
    const pending = externalTranscriptMessages(
      parseClaudeTranscriptEntries(contents.value),
      evidence,
      importedThroughMs,
    ).filter((message) => !existingItemIds.has(syncTurnItemId(threadId, message.uuid)));

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
    return { _tag: "Synced", appendedCount, source } as const;
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
    readonly lastSeen: ReturnType<typeof AgentSessionScanner.transcriptIdentity> | null;
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
    const stats = yield* fileSystem.stat(entry.recorded.filePath).pipe(Effect.option);
    if (Option.isNone(stats)) return;
    const identity = AgentSessionScanner.transcriptIdentity(entry.recorded.filePath, stats.value);
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
      lastSeen: null,
    }));
  });

  yield* forkParked(
    Ref.get(watched).pipe(
      Effect.flatMap((current) =>
        Effect.forEach(current, ([threadId, entry]) => pollThread(threadId, entry), {
          discard: true,
        }),
      ),
      Effect.catchCause((cause) => Effect.logWarning("Imported transcript poll failed", { cause })),
      Effect.repeat(Schedule.spaced(TRANSCRIPT_POLL_INTERVAL)),
    ),
  );

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

  return ImportedTranscriptSync.of({ syncThread, watch });
});

export const layer = Layer.effect(ImportedTranscriptSync, make);
