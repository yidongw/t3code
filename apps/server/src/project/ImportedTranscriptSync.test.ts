import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import {
  EventId,
  MessageId,
  NodeId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ProjectId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import * as TurnItemPositionStore from "../orchestration-v2/TurnItemPositionStore.ts";
import { CLAUDE_MODEL_SELECTION } from "../orchestration-v2/testkit/fixtures/shared.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ImportedTranscriptSync from "./ImportedTranscriptSync.ts";

const sessionId = "020e7f00-dce6-406b-8ec6-ed90498945d6";
const instanceId = CLAUDE_MODEL_SELECTION.instanceId;
const threadId = ThreadId.make(`import:${instanceId}:${sessionId}`);
const providerThreadId = ProviderThreadId.make("provider-thread:claude");
const providerSessionId = ProviderSessionId.make("provider-session:claude");
const at = (time: string) => `2026-09-01T${time}.000Z`;
const utc = (time: string) => DateTime.makeUnsafe(at(time));

const stores = Layer.mergeAll(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const persistence = Layer.mergeAll(
  stores,
  EventSink.layer.pipe(Layer.provide(stores)),
  ProviderSessionRuntime.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  TurnItemPositionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  ProjectStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
  ThreadSearch.layer.pipe(Layer.provide(stores)),
  NodeServices.layer,
);

function line(
  type: "user" | "assistant",
  uuid: string,
  time: string,
  content: string | ReadonlyArray<Record<string, unknown>>,
) {
  return `${JSON.stringify({
    type,
    uuid,
    sessionId,
    timestamp: at(time),
    message: {
      role: type,
      content: typeof content === "string" ? [{ type: "text", text: content }] : content,
    },
  })}\n`;
}

const imported =
  line("user", "u1", "10:00:00", "hi") + line("assistant", "a1", "10:00:01", "hello");
// T3's own turn: its last assistant uuid is the provider turn's native ref.
const t3Turn =
  line("user", "u2", "10:05:00", "t3 prompt") + line("assistant", "a2", "10:05:10", "t3 answer");
const discordTurn =
  line("user", "u3", "10:10:00", "discord question") +
  line("assistant", "a3-tool", "10:10:01", [{ type: "tool_use", id: "tool", name: "Bash" }]) +
  line("user", "u3-result", "10:10:02", [{ type: "tool_result", tool_use_id: "tool" }]) +
  line("assistant", "a3", "10:10:05", "discord answer");
// A T3 turn that failed before any assistant output is recognised by its time window.
const t3FailedTurn = line("user", "u4", "10:20:00", "t3 failed prompt");
const laterDiscordTurn =
  line("user", "u5", "10:30:00", "later question") +
  line("assistant", "a5", "10:30:02", "later answer");

function run(ordinal: number, status: OrchestrationV2Run["status"], startedAt: string) {
  return {
    id: RunId.make(`run:${ordinal}`),
    threadId,
    ordinal,
    providerInstanceId: instanceId,
    modelSelection: CLAUDE_MODEL_SELECTION,
    providerThreadId,
    userMessageId: MessageId.make(`message:run:${ordinal}`),
    rootNodeId: NodeId.make(`node:run:${ordinal}`),
    activeAttemptId: RunAttemptId.make(`attempt:${ordinal}`),
    status,
    requestedAt: utc(startedAt),
    startedAt: utc(startedAt),
    completedAt: status === "running" ? null : utc(startedAt),
    checkpointId: null,
    contextHandoffId: null,
  } satisfies OrchestrationV2Run;
}

function runEvents(input: {
  readonly ordinal: number;
  readonly status: OrchestrationV2Run["status"];
  readonly startedAt: string;
  readonly completedAt: string | null;
  readonly nativeTurnId: string;
  readonly items?: ReadonlyArray<{ readonly role: "user" | "assistant"; readonly text: string }>;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const payload = run(input.ordinal, input.status, input.startedAt);
  const occurredAt = utc(input.startedAt);
  return [
    {
      id: EventId.make(`run:${input.ordinal}:${input.status}`),
      type: "run.updated",
      threadId,
      occurredAt,
      payload,
    },
    {
      id: EventId.make(`provider-turn:${input.ordinal}:${input.status}`),
      type: "provider-turn.updated",
      threadId,
      occurredAt,
      payload: {
        id: ProviderTurnId.make(`provider-turn:${input.ordinal}`),
        providerThreadId,
        nodeId: payload.rootNodeId!,
        runAttemptId: payload.activeAttemptId,
        nativeTurnRef: {
          driver: ProviderDriverKind.make("claudeAgent"),
          nativeId: input.nativeTurnId,
          strength: "weak",
        },
        ordinal: input.ordinal,
        status: input.status === "running" ? "running" : "completed",
        startedAt: occurredAt,
        completedAt: input.completedAt === null ? null : utc(input.completedAt),
      },
    },
    ...(input.items ?? []).map((item, index): OrchestrationV2DomainEvent => ({
      id: EventId.make(`run:${input.ordinal}:item:${index}`),
      type: "turn-item.updated",
      threadId,
      occurredAt,
      payload: {
        id: TurnItemId.make(`run:${input.ordinal}:item:${index}`),
        threadId,
        runId: payload.id,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: input.ordinal * 100 + index,
        status: "completed",
        title: null,
        startedAt: occurredAt,
        completedAt: occurredAt,
        updatedAt: occurredAt,
        ...(item.role === "user"
          ? {
              type: "user_message" as const,
              createdBy: "user" as const,
              creationSource: "web" as const,
              messageId: MessageId.make(`run:${input.ordinal}:message:${index}`),
              inputIntent: "turn_start" as const,
              text: item.text,
              attachments: [],
            }
          : {
              type: "assistant_message" as const,
              messageId: MessageId.make(`run:${input.ordinal}:message:${index}`),
              text: item.text,
              streaming: false,
            }),
      },
    })),
  ];
}

// Search lists threads of projects it knows.
const createSearchableProject = Effect.gen(function* () {
  yield* (yield* ProjectStore.ProjectStoreV2).apply({
    sequence: 0,
    eventId: EventId.make("project:created"),
    aggregateKind: "project",
    aggregateId: ProjectId.make("project"),
    occurredAt: at("10:00:00"),
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "project.created",
    payload: {
      projectId: ProjectId.make("project"),
      title: "Repo",
      workspaceRoot: "/repo",
      defaultModelSelection: null,
      scripts: [],
      createdAt: at("10:00:00"),
      updatedAt: at("10:00:00"),
    },
  });
});

interface ImportedHistory {
  /** The transcript as the import found it. */
  readonly transcript: string;
  /** What the import kept of it. */
  readonly messages: ReadonlyArray<{
    readonly role: "user" | "assistant";
    readonly text: string;
    readonly time: string;
  }>;
  readonly omittedCount: number;
}

const defaultHistory: ImportedHistory = {
  transcript: imported,
  messages: [
    { role: "user", text: "hi", time: "10:00:00" },
    { role: "assistant", text: "hello", time: "10:00:01" },
  ],
  omittedCount: 0,
};

const setup = Effect.fn(function* (options?: {
  readonly maxSyncBytes?: number;
  readonly maxBackfillStepBytes?: number;
  readonly maxBackfillStepMessages?: number;
  readonly history?: ImportedHistory;
}) {
  const history = options?.history ?? defaultHistory;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const sink = yield* EventSink.EventSinkV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-transcript-sync-" });
  const claudeHome = path.join(directory, "claude");
  const workspaceRoot = path.join(directory, "repo");
  // Claude Code keeps a session under a directory named after the cwd it ran in.
  const transcriptIn = (projectDir: string) =>
    path.join(claudeHome, "projects", projectDir, `${sessionId}.jsonl`);
  const transcriptFor = (cwd: string) => transcriptIn(cwd.replace(/[^a-zA-Z0-9]/g, "-"));
  const writeTranscript = (target: string, contents: string) =>
    fileSystem
      .makeDirectory(path.dirname(target), { recursive: true })
      .pipe(Effect.andThen(fileSystem.writeFileString(target, contents)));
  const filePath = transcriptFor(workspaceRoot);
  yield* writeTranscript(filePath, history.transcript);
  const releases: Array<ProviderSessionId> = [];
  // What the service asked of the file system, to show what a sweep of unchanged threads costs.
  const fileSystemCalls: Array<string> = [];

  const created = utc("10:00:00");
  const providerThread: OrchestrationV2ProviderThread = {
    id: providerThreadId,
    driver: ProviderDriverKind.make("claudeAgent"),
    providerInstanceId: instanceId,
    providerSessionId,
    appThreadId: threadId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: ProviderDriverKind.make("claudeAgent"),
      nativeId: sessionId,
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: created,
    updatedAt: created,
  };
  const thread: OrchestrationV2AppThread = {
    createdBy: "system",
    creationSource: "server",
    id: threadId,
    projectId: ProjectId.make("project"),
    title: "Imported",
    providerInstanceId: instanceId,
    modelSelection: CLAUDE_MODEL_SELECTION,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: providerThreadId,
    historyOrigin: "v1_import",
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: created,
    updatedAt: created,
    archivedAt: null,
    settledOverride: "settled",
    settledAt: created,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* sink.write({
    events: [
      {
        id: EventId.make("thread:created"),
        type: "thread.created",
        threadId,
        occurredAt: created,
        payload: thread,
      },
      // As the importer writes them: the notice first, then what it kept.
      ...(history.omittedCount === 0
        ? []
        : [
            {
              id: EventId.make("notice"),
              type: "turn-item.updated" as const,
              threadId,
              occurredAt: created,
              payload: {
                id: ImportedTranscriptSync.historyNoticeItemId(threadId),
                threadId,
                runId: null,
                nodeId: null,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 0,
                status: "completed" as const,
                title: null,
                startedAt: created,
                completedAt: created,
                updatedAt: created,
                type: "system_notice" as const,
                message: `${history.omittedCount} earlier messages were not imported.`,
              },
            },
          ]),
      ...history.messages.flatMap((message, index) => {
        const key = String(index).padStart(6, "0");
        return ImportedTranscriptSync.importedMessageEvents({
          threadId,
          prefix: ImportedTranscriptSync.IMPORT_EVENT_PREFIX,
          messageKey: key,
          itemKey: key,
          ordinal: index + 1,
          message: { role: message.role, text: message.text, createdAt: at(message.time) },
        });
      }),
      {
        id: EventId.make("provider-thread"),
        type: "provider-thread.updated",
        threadId,
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        occurredAt: created,
        payload: providerThread,
      },
    ],
  });
  yield* runtimes.upsert({
    threadId,
    providerName: ProviderDriverKind.make("claudeAgent"),
    providerInstanceId: instanceId,
    adapterKey: ProviderDriverKind.make("claudeAgent"),
    runtimeMode: "full-access",
    status: "stopped",
    lastSeenAt: at("10:00:01"),
    resumeCursor: { threadId, resume: sessionId },
    runtimePayload: { cwd: directory },
  });
  yield* runtimes.recordImportedTranscript({
    threadId,
    source: {
      ...AgentSessionScanner.transcriptIdentity(filePath, yield* fileSystem.stat(filePath)),
      provider: "claudeAgent",
      providerInstanceId: instanceId,
      providerSessionId: sessionId,
    },
  });

  const build = Layer.build(
    ImportedTranscriptSync.layerWith(options ?? {}).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(EventSink.EventSinkV2, sink),
          Layer.succeed(
            ProjectionStore.ProjectionStoreV2,
            yield* ProjectionStore.ProjectionStoreV2,
          ),
          Layer.succeed(ProviderSessionRuntime.ProviderSessionRuntimeRepository, runtimes),
          Layer.succeed(FileSystem.FileSystem, {
            ...fileSystem,
            stat: (target) => {
              fileSystemCalls.push(`stat ${target}`);
              return fileSystem.stat(target);
            },
            open: (target, openOptions) => {
              fileSystemCalls.push(`open ${target}`);
              return fileSystem.open(target, openOptions);
            },
          }),
          Layer.succeed(Path.Path, path),
          ServerSettings.layerTest({
            providerInstances: {
              [instanceId]: {
                driver: ProviderDriverKind.make("claudeAgent"),
                config: { homePath: claudeHome },
              },
            },
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            get: (projectId) =>
              Effect.succeedSome({
                projectId,
                title: "Repo",
                workspaceRoot,
                defaultModelSelection: null,
                defaultThreadEnvMode: null,
                autoPull: false,
                faviconPath: null,
                projectIcon: null,
                scripts: [],
                pinnedAt: null,
                archivedAt: null,
                createdAt: at("10:00:00"),
                updatedAt: at("10:00:00"),
                deletedAt: null,
              }),
          }),
          Layer.succeed(
            TurnItemPositionStore.TurnItemPositionStoreV2,
            yield* TurnItemPositionStore.TurnItemPositionStoreV2,
          ),
          Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
            isLive: () => Effect.succeed(true),
            releaseUnlessBusy: (input) =>
              Effect.sync(() => {
                releases.push(input.providerSessionId);
                return true;
              }),
          }),
        ),
      ),
    ),
  ).pipe(
    Effect.map((context) => Context.get(context, ImportedTranscriptSync.ImportedTranscriptSync)),
  );
  const sync = yield* build;
  const append = (text: string) =>
    fileSystem
      .readFileString(filePath)
      .pipe(Effect.flatMap((current) => fileSystem.writeFileString(filePath, current + text)));
  const conversation = Effect.gen(function* () {
    const { turnItems } = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(
      threadId,
      ["turnItems"],
    );
    return turnItems.flatMap((item) =>
      item.type === "user_message" || item.type === "assistant_message"
        ? [`${item.ordinal}:${item.text}`]
        : [],
    );
  });
  const timeline = Effect.gen(function* () {
    const { turnItems } = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadRecords(
      threadId,
      ["turnItems"],
    );
    return turnItems
      .toSorted((left, right) => left.ordinal - right.ordinal)
      .flatMap((item) =>
        item.type === "user_message" || item.type === "assistant_message"
          ? [`${item.ordinal}:${item.text}`]
          : item.type === "system_notice"
            ? [`${item.ordinal}:notice:${item.message}`]
            : [],
      );
  });
  const moveToWorktree = (worktreePath: string) =>
    sink.write({
      events: [
        {
          id: EventId.make(`thread:worktree:${worktreePath}`),
          type: "thread.metadata-updated",
          threadId,
          occurredAt: utc("10:50:00"),
          payload: { ...thread, branch: "feature", worktreePath, updatedAt: utc("10:50:00") },
        },
      ],
    });
  const recordedPath = sync
    .syncThread(threadId)
    .pipe(Effect.map((result) => ("source" in result ? result.source.filePath : null)));
  return {
    sync,
    build,
    thread,
    timeline,
    sink,
    fileSystemCalls,
    append,
    conversation,
    releases,
    fileSystem,
    filePath,
    transcriptIn,
    transcriptFor,
    writeTranscript,
    moveToWorktree,
    recordedPath,
  };
});

it.effect(
  "appends other clients' turns in transcript order without duplicating T3's own turns",
  () =>
    Effect.gen(function* () {
      const { sync, sink, append, conversation, releases } = yield* setup();
      expect((yield* sync.syncThread(threadId))._tag).toBe("Unchanged");

      // T3 is running a turn: never sync while it writes.
      yield* sink.write({
        events: runEvents({
          ordinal: 1,
          status: "running",
          startedAt: "10:04:59",
          completedAt: null,
          nativeTurnId: "turn:attempt:1",
        }),
      });
      yield* append(t3Turn);
      expect((yield* sync.syncThread(threadId))._tag).toBe("Busy");
      yield* sink.write({
        events: runEvents({
          ordinal: 1,
          status: "completed",
          startedAt: "10:04:59",
          completedAt: "10:05:11",
          nativeTurnId: "a2",
          items: [
            { role: "user", text: "t3 prompt" },
            { role: "assistant", text: "t3 answer" },
          ],
        }),
      });
      yield* append(discordTurn);
      yield* append(t3FailedTurn);
      yield* sink.write({
        events: runEvents({
          ordinal: 2,
          status: "failed",
          startedAt: "10:20:00",
          completedAt: "10:20:00",
          nativeTurnId: "turn:attempt:2",
          items: [{ role: "user", text: "t3 failed prompt" }],
        }),
      });
      yield* append(laterDiscordTurn);
      // A record still being written waits for its newline.
      yield* append(line("user", "u6", "10:40:00", "partial").slice(0, 20));

      expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 4 });
      expect(yield* conversation).toEqual([
        "1:hi",
        "2:hello",
        "1000001:t3 prompt",
        "1000002:t3 answer",
        "1000003:discord question",
        "1000004:discord answer",
        "2000001:t3 failed prompt",
        "2000002:later question",
        "2000003:later answer",
      ]);
      expect(releases).toEqual([providerSessionId]);

      // Unchanged, then forced: neither appends again.
      expect((yield* sync.syncThread(threadId))._tag).toBe("Unchanged");
      const recorded = yield* sync.syncThread(threadId);
      expect(recorded._tag).toBe("Unchanged");
      if (recorded._tag !== "Unchanged") return;
      expect(yield* sync.syncThread(threadId, { source: recorded.source })).toMatchObject({
        _tag: "Synced",
        appendedCount: 0,
      });
      expect((yield* conversation).length).toBe(9);
      expect(releases).toHaveLength(1);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("treats a truncated or rewritten transcript as a safe no-op", () =>
  Effect.gen(function* () {
    const { sync, append, conversation, fileSystem, filePath } = yield* setup();
    yield* append(discordTurn);
    const first = yield* sync.syncThread(threadId);
    expect(first).toMatchObject({ _tag: "Synced", appendedCount: 2 });
    if (first._tag !== "Synced") return;
    const before = yield* conversation;

    yield* fileSystem.writeFileString(filePath, line("user", "u1", "10:00:00", "hi"));
    expect(yield* sync.syncThread(threadId, { source: first.source })).toMatchObject({
      _tag: "Synced",
      appendedCount: 0,
    });
    yield* fileSystem.writeFileString(filePath, imported + discordTurn);
    expect(yield* sync.syncThread(threadId, { source: first.source })).toMatchObject({
      _tag: "Synced",
      appendedCount: 0,
    });
    expect(yield* conversation).toEqual(before);

    // A new file of the same size in its place is read from the start, not the old offset.
    const replacement = `${filePath}.new`;
    const anotherTurn = (answer: string) =>
      imported +
      line("user", "v3", "10:10:00", "another question") +
      line("assistant", "b3", "10:10:05", answer);
    const padding = (imported + discordTurn).length - anotherTurn("another answer").length;
    yield* fileSystem.writeFileString(
      replacement,
      anotherTurn(`another answer ${"!".repeat(padding - 1)}`),
    );
    yield* fileSystem.rename(replacement, filePath);
    expect((yield* fileSystem.stat(filePath)).size).toBe(BigInt((imported + discordTurn).length));
    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 2 });
    expect((yield* conversation).slice(before.length)).toEqual([
      "5:another question",
      expect.stringMatching(/^6:another answer/),
    ]);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("classifies a turn split across two syncs once, as the segment it continues", () =>
  Effect.gen(function* () {
    const { sync, sink, append, conversation } = yield* setup();
    // T3's prompt is synced before the record Claude flushes after the run settled.
    yield* sink.write({
      events: runEvents({
        ordinal: 1,
        status: "completed",
        startedAt: "10:04:59",
        completedAt: "10:05:11",
        nativeTurnId: "turn:attempt:1",
        items: [{ role: "user", text: "t3 prompt" }],
      }),
    });
    yield* append(line("user", "u2", "10:05:00", "t3 prompt"));
    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 0 });
    yield* append(line("assistant", "a2", "10:05:30", "t3 late answer"));
    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 0 });

    // Another client's prompt is synced before its reply.
    yield* append(
      line("user", "u3", "10:10:00", "discord question") +
        line("assistant", "a3-tool", "10:10:01", [{ type: "tool_use", id: "tool", name: "Bash" }]),
    );
    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 1 });
    yield* append(
      line("user", "u3-result", "10:10:02", [{ type: "tool_result", tool_use_id: "tool" }]) +
        line("assistant", "a3", "10:10:05", "discord answer"),
    );
    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 1 });

    expect(yield* conversation).toEqual([
      "1:hi",
      "2:hello",
      "1000001:t3 prompt",
      "1000002:discord question",
      "1000003:discord answer",
    ]);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("catches up a transcript past the per-sync budget, then reads only appended bytes", () =>
  Effect.gen(function* () {
    const maxSyncBytes = 4 * 1024;
    const { sync, append, conversation } = yield* setup({ maxSyncBytes });
    // Non-conversation records from before the import, many budgets long.
    const filler = Array.from(
      { length: 64 },
      (_, index) =>
        `${JSON.stringify({ type: "progress", uuid: `p${index}`, data: "x".repeat(1024) })}\n`,
    ).join("");
    yield* append(filler + discordTurn);

    let syncs = 0;
    let result = yield* sync.syncThread(threadId);
    let appended = 0;
    while (result._tag === "Synced" && syncs < 100) {
      syncs += 1;
      appended += result.appendedCount;
      if (result.caughtUp) break;
      result = yield* sync.syncThread(threadId);
    }
    expect(result).toMatchObject({ _tag: "Synced", caughtUp: true });
    expect(syncs).toBeGreaterThan(filler.length / maxSyncBytes);
    expect(appended).toBe(2);

    // A re-read from the start would stop at the budget, long before the new turn.
    yield* append(laterDiscordTurn);
    expect(yield* sync.syncThread(threadId)).toMatchObject({
      _tag: "Synced",
      appendedCount: 2,
      caughtUp: true,
    });
    expect(yield* conversation).toEqual([
      "1:hi",
      "2:hello",
      "3:discord question",
      "4:discord answer",
      "5:later question",
      "6:later answer",
    ]);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("follows the transcript to the thread's worktree once the recorded copy is gone", () =>
  Effect.gen(function* () {
    const { sync, conversation, fileSystem, filePath, transcriptFor, writeTranscript, ...thread } =
      yield* setup();
    const worktreeTranscript = transcriptFor("/work/repo-feature");
    yield* thread.moveToWorktree("/work/repo-feature");
    yield* writeTranscript(worktreeTranscript, imported + discordTurn);
    yield* fileSystem.remove(filePath);

    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 2 });
    expect(yield* conversation).toEqual([
      "1:hi",
      "2:hello",
      "3:discord question",
      "4:discord answer",
    ]);
    expect(yield* thread.recordedPath).toBe(worktreeTranscript);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("a watched thread picks up its transcript after its worktree changes", () =>
  Effect.gen(function* () {
    const { sync, conversation, transcriptFor, writeTranscript, moveToWorktree, recordedPath } =
      yield* setup();
    yield* sync.watch(threadId);
    yield* sync.pollWatched;

    // Claude starts a new transcript under the worktree; the old one stays behind.
    const worktreeTranscript = transcriptFor("/work/repo-feature");
    yield* moveToWorktree("/work/repo-feature");
    yield* writeTranscript(worktreeTranscript, imported + discordTurn);
    yield* sync.pollWatched;
    expect(yield* conversation).toHaveLength(2);
    // Synced once the file stayed unchanged for one interval.
    yield* sync.pollWatched;
    expect(yield* conversation).toEqual([
      "1:hi",
      "2:hello",
      "3:discord question",
      "4:discord answer",
    ]);
    expect(yield* recordedPath).toBe(worktreeTranscript);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("warns once while no transcript exists and recovers when one appears", () => {
  const warnings: Array<string> = [];
  const logger = Logger.make(({ message }) => {
    const text = JSON.stringify(message);
    if (text.includes("Imported Claude transcript not found")) warnings.push(text);
  });
  return Effect.gen(function* () {
    const { sync, conversation, fileSystem, filePath, transcriptIn, writeTranscript, ...thread } =
      yield* setup();
    yield* fileSystem.remove(filePath);
    yield* sync.watch(threadId);
    yield* sync.pollWatched;
    yield* sync.pollWatched;
    yield* sync.pollWatched;
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain(filePath);

    // Only a search of every project directory finds a copy under an unexpected name.
    const elsewhere = transcriptIn("-private-repo");
    yield* writeTranscript(elsewhere, imported + discordTurn);
    expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 2 });
    expect(yield* thread.recordedPath).toBe(elsewhere);
    expect(yield* conversation).toHaveLength(4);
    expect(warnings).toHaveLength(1);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(persistence, Logger.layer([logger], { mergeWithExisting: false })),
    ),
  );
});

it.effect(
  "a sweep syncs an imported thread nobody has open, at the cost of one stat while unchanged",
  () =>
    Effect.gen(function* () {
      const { sync, append, conversation, fileSystemCalls, filePath, releases } = yield* setup();
      expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
      fileSystemCalls.length = 0;
      expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
      expect(fileSystemCalls).toEqual([`stat ${filePath}`]);

      yield* append(discordTurn);
      expect(yield* sync.sweepImported).toEqual({ threadsSynced: 1, messagesAppended: 2 });
      expect(yield* conversation).toEqual([
        "1:hi",
        "2:hello",
        "3:discord question",
        "4:discord answer",
      ]);
      expect(releases).toEqual([providerSessionId]);

      fileSystemCalls.length = 0;
      expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
      expect(fileSystemCalls).toEqual([`stat ${filePath}`]);

      yield* createSearchableProject;
      const { matches } = yield* (yield* ThreadSearch.ThreadSearch).search({
        query: "discord question",
      });
      expect(matches).toMatchObject([
        { threadId, snippet: expect.stringContaining("discord question") },
      ]);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "the first sweep after a restart catches up turns written while the server was down",
  () =>
    Effect.gen(function* () {
      const { build, append, conversation } = yield* setup();
      yield* append(discordTurn);
      yield* append(laterDiscordTurn);
      const restarted = yield* build;
      expect(yield* restarted.sweepImported).toEqual({ threadsSynced: 1, messagesAppended: 4 });
      expect(yield* conversation).toEqual([
        "1:hi",
        "2:hello",
        "3:discord question",
        "4:discord answer",
        "5:later question",
        "6:later answer",
      ]);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("a sweep leaves a thread alone while T3 runs it and syncs it once the run is over", () =>
  Effect.gen(function* () {
    const { sync, sink, append, conversation, fileSystemCalls } = yield* setup();
    const runningRun = { ordinal: 1, startedAt: "10:04:59", nativeTurnId: "a2" } as const;
    yield* sink.write({
      events: runEvents({ ...runningRun, status: "running", completedAt: null }),
    });
    yield* append(t3Turn);
    fileSystemCalls.length = 0;
    expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
    expect(fileSystemCalls.filter((call) => call.startsWith("open "))).toEqual([]);

    yield* sink.write({
      events: runEvents({
        ...runningRun,
        status: "completed",
        completedAt: "10:05:11",
        items: [
          { role: "user", text: "t3 prompt" },
          { role: "assistant", text: "t3 answer" },
        ],
      }),
    });
    yield* append(discordTurn);
    expect(yield* sync.sweepImported).toEqual({ threadsSynced: 1, messagesAppended: 2 });
    expect(yield* conversation).toEqual([
      "1:hi",
      "2:hello",
      "1000001:t3 prompt",
      "1000002:t3 answer",
      "1000003:discord question",
      "1000004:discord answer",
    ]);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect.each(["thread.archived", "thread.deleted"] as const)(
  "a sweep skips a thread after %s",
  (type) =>
    Effect.gen(function* () {
      const { sync, sink, thread, append, conversation, fileSystemCalls } = yield* setup();
      yield* sink.write({
        events: [
          {
            id: EventId.make(type),
            type,
            threadId,
            occurredAt: utc("10:01:00"),
            payload: {
              ...thread,
              ...(type === "thread.archived"
                ? { archivedAt: utc("10:01:00") }
                : { deletedAt: utc("10:01:00") }),
              updatedAt: utc("10:01:00"),
            },
          },
        ],
      });
      yield* append(discordTurn);
      expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
      // Until the thread is resolved again, later sweeps do not even stat its transcript.
      fileSystemCalls.length = 0;
      expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
      expect(fileSystemCalls).toEqual([]);
      expect(yield* conversation).toEqual(["1:hi", "2:hello"]);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect("a sweep never touches the transcripts of Codex or non-imported threads", () =>
  Effect.gen(function* () {
    const { sync, fileSystem, filePath, fileSystemCalls, writeTranscript, transcriptIn } =
      yield* setup();
    const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
    const otherPath = transcriptIn("-other");
    yield* writeTranscript(otherPath, imported);
    const recordedOther = AgentSessionScanner.transcriptIdentity(
      otherPath,
      yield* fileSystem.stat(otherPath),
    );
    yield* writeTranscript(otherPath, imported + discordTurn);
    const others = [
      { threadId: ThreadId.make(`import:codex:${sessionId}`), provider: "codex" },
      { threadId: ThreadId.make("thread:native"), provider: "claudeAgent" },
    ] as const;
    for (const other of others) {
      yield* runtimes.upsert({
        threadId: other.threadId,
        providerName: ProviderDriverKind.make(other.provider),
        providerInstanceId: instanceId,
        adapterKey: ProviderDriverKind.make(other.provider),
        runtimeMode: "full-access",
        status: "stopped",
        lastSeenAt: at("10:00:01"),
        resumeCursor: null,
        runtimePayload: {
          importedTranscripts: [
            {
              ...recordedOther,
              provider: other.provider,
              providerInstanceId: instanceId,
              providerSessionId: sessionId,
            },
          ],
        },
      });
    }
    fileSystemCalls.length = 0;
    expect(yield* sync.sweepImported).toEqual({ threadsSynced: 0, messagesAppended: 0 });
    expect(fileSystemCalls).toEqual([`stat ${filePath}`]);
  }).pipe(Effect.scoped, Effect.provide(persistence)),
);

function hiddenLine(
  flag: "isMeta" | "isCompactSummary" | "isSidechain",
  uuid: string,
  time: string,
) {
  return `${JSON.stringify({
    type: flag === "isSidechain" ? "assistant" : "user",
    uuid,
    sessionId,
    timestamp: at(time),
    [flag]: true,
    message: { content: [{ type: "text", text: `${flag} text` }] },
  })}\n`;
}

// An import that kept the first prompt and the latest two messages of this history.
const truncatedHistory: ImportedHistory = {
  transcript:
    line("assistant", "b0", "09:59:00", "resumed context") +
    line("user", "b1", "10:00:00", "first prompt") +
    line("assistant", "b2", "10:00:01", "dropped answer") +
    hiddenLine("isMeta", "b3", "10:00:01") +
    line("user", "b4", "10:00:02", "dropped prompt") +
    hiddenLine("isSidechain", "b5", "10:00:02") +
    line("assistant", "b6", "10:00:03", [{ type: "tool_use", id: "tool", name: "Bash" }]) +
    hiddenLine("isCompactSummary", "b7", "10:00:03") +
    line("assistant", "b8", "10:00:04", "dropped reply") +
    line("user", "b9", "10:00:05", "window prompt") +
    line("assistant", "b10", "10:00:06", "window answer"),
  messages: [
    { role: "user", text: "first prompt", time: "10:00:00" },
    { role: "user", text: "window prompt", time: "10:00:05" },
    { role: "assistant", text: "window answer", time: "10:00:06" },
  ],
  omittedCount: 4,
};

const backfillToEnd = (sync: ImportedTranscriptSync.ImportedTranscriptSyncShape) =>
  Effect.gen(function* () {
    const steps: Array<ImportedTranscriptSync.ImportedHistoryBackfillStep> = [];
    let step = yield* sync.backfillStep(threadId);
    while (step._tag === "Progress" && steps.length < 100) {
      steps.push(step);
      step = yield* sync.backfillStep(threadId);
    }
    return { steps, last: step };
  });

it.effect(
  "backfills what an import left out in transcript order, around synced items and T3 runs",
  () =>
    Effect.gen(function* () {
      const { sync, build, sink, append, timeline } = yield* setup({ history: truncatedHistory });
      // Another client's turn, a T3 run, then another client's turn after it.
      yield* append(discordTurn);
      expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 2 });
      yield* sink.write({
        events: runEvents({
          ordinal: 1,
          status: "completed",
          startedAt: "10:20:00",
          completedAt: "10:20:11",
          nativeTurnId: "t3-a",
          items: [
            { role: "user", text: "t3 prompt" },
            { role: "assistant", text: "t3 answer" },
          ],
        }),
      });
      yield* append(
        line("user", "t3-u", "10:20:01", "t3 prompt") +
          line("assistant", "t3-a", "10:20:10", "t3 answer"),
      );
      yield* append(laterDiscordTurn);
      expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 2 });
      const after = [
        "1000001:t3 prompt",
        "1000002:t3 answer",
        "1000003:later question",
        "1000004:later answer",
      ];
      expect(yield* timeline).toEqual([
        "1:notice:4 earlier messages were not imported.",
        "2:first prompt",
        "3:window prompt",
        "4:window answer",
        "5:discord question",
        "6:discord answer",
        ...after,
      ]);

      const { steps, last } = yield* backfillToEnd(sync);
      expect(steps).toEqual([{ _tag: "Progress", phase: "relayout", addedCount: 0 }]);
      expect(last).toEqual({ _tag: "Done", addedCount: 4 });
      const backfilled = [
        "7:resumed context",
        "8:first prompt",
        "9:dropped answer",
        "10:dropped prompt",
        "11:dropped reply",
        "12:window prompt",
        "13:window answer",
        "14:discord question",
        "15:discord answer",
        ...after,
      ];
      const complete = yield* timeline;
      expect(complete.slice(1)).toEqual(backfilled);
      expect(complete[0]).toMatch(/^1:notice:Earlier history was imported/);

      // Repeats, a restarted server and the next sync add nothing twice.
      const events = yield* EventStore.EventStoreV2;
      const eventCount = (yield* events.read({ threadId }).pipe(Stream.runCollect)).length;
      expect(yield* sync.backfillStep(threadId)).toEqual({ _tag: "Done", addedCount: 4 });
      expect(yield* (yield* build).backfillStep(threadId)).toEqual({
        _tag: "Done",
        addedCount: 4,
      });
      expect((yield* events.read({ threadId }).pipe(Stream.runCollect)).length).toBe(eventCount);
      yield* append(line("user", "v7", "10:40:00", "after the backfill"));
      expect(yield* sync.syncThread(threadId)).toMatchObject({ _tag: "Synced", appendedCount: 1 });
      expect((yield* timeline).slice(1)).toEqual([...backfilled, "1000005:after the backfill"]);

      // Backfilled messages are indexed like any other.
      yield* createSearchableProject;
      const { matches } = yield* (yield* ThreadSearch.ThreadSearch).search({
        query: "dropped reply",
      });
      expect(matches).toMatchObject([
        { threadId, snippet: expect.stringContaining("dropped reply") },
      ]);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

it.effect(
  "resumes a backfill after a restart, one bounded step at a time, never during a run",
  () =>
    Effect.gen(function* () {
      const dropped = Array.from({ length: 12 }, (_, index) =>
        line(
          index % 2 === 0 ? "user" : "assistant",
          `d${index}`,
          `10:00:${String(10 + index).padStart(2, "0")}`,
          `dropped ${index}`,
        ),
      ).join("");
      const filler = `${JSON.stringify({ type: "progress", uuid: "p", data: "x".repeat(2048) })}\n`;
      const history: ImportedHistory = {
        transcript:
          line("user", "f", "10:00:00", "first prompt") +
          dropped +
          filler +
          line("user", "w1", "10:01:00", "window prompt") +
          line("assistant", "w2", "10:01:01", "window answer"),
        messages: [
          { role: "user", text: "first prompt", time: "10:00:00" },
          { role: "user", text: "window prompt", time: "10:01:00" },
          { role: "assistant", text: "window answer", time: "10:01:01" },
        ],
        omittedCount: 12,
      };
      const budget = { maxBackfillStepBytes: 1024, maxBackfillStepMessages: 2 };
      const { sync, build, sink, timeline } = yield* setup({ ...budget, history });
      const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const saved = runtimes.getByThreadId({ threadId }).pipe(
        Effect.map(
          (runtime) =>
            (
              Option.getOrThrow(runtime).runtimePayload as {
                readonly importedHistoryBackfill: {
                  readonly phase: string;
                  readonly offset: number;
                };
              }
            ).importedHistoryBackfill,
        ),
      );

      // Numbering the transcript takes several reads of at most about 1 KiB.
      const planned: Array<ImportedTranscriptSync.ImportedHistoryBackfillStep> = [];
      let step = yield* sync.backfillStep(threadId);
      while (step._tag === "Progress" && step.phase === "plan") {
        planned.push(step);
        step = yield* sync.backfillStep(threadId);
      }
      expect(planned.length).toBeGreaterThan(1);
      expect(step).toEqual({ _tag: "Progress", phase: "relayout", addedCount: 0 });

      const firstFill = yield* sync.backfillStep(threadId);
      expect(firstFill).toEqual({ _tag: "Progress", phase: "fill", addedCount: 3 });
      const { offset } = yield* saved;

      // A T3 run holds the backfill back until it is over.
      const runningRun = {
        ordinal: 1,
        startedAt: "10:30:00",
        nativeTurnId: "turn:attempt:1",
      } as const;
      yield* sink.write({
        events: runEvents({ ...runningRun, status: "running", completedAt: null }),
      });
      expect(yield* sync.backfillStep(threadId)).toEqual({ _tag: "Busy" });
      yield* sink.write({
        events: runEvents({ ...runningRun, status: "completed", completedAt: "10:30:05" }),
      });

      // A restarted server continues from the saved offset. A step stops at its budget and adds
      // the messages of the turn it stopped in.
      const restarted = yield* build;
      const resumed = yield* restarted.backfillStep(threadId);
      expect(resumed).toMatchObject({ _tag: "Progress", phase: "fill" });
      expect((yield* saved).offset).toBeGreaterThan(offset);
      const rest = yield* backfillToEnd(restarted);
      const fills = [firstFill, resumed, ...rest.steps];
      for (const fill of fills) {
        expect(fill).toMatchObject({ _tag: "Progress", phase: "fill" });
        if (fill._tag !== "Progress") continue;
        expect(fill.addedCount).toBeLessThanOrEqual(budget.maxBackfillStepMessages + 2);
      }
      expect(rest.last).toEqual({ _tag: "Done", addedCount: 12 });
      expect((yield* saved).phase).toBe("done");

      expect((yield* timeline).slice(1)).toEqual([
        "5:first prompt",
        ...Array.from({ length: 12 }, (_, index) => `${6 + index}:dropped ${index}`),
        "18:window prompt",
        "19:window answer",
      ]);
    }).pipe(Effect.scoped, Effect.provide(persistence)),
);

describe("sweepsPerPeriod", () => {
  it("turns a wall-clock period into a sweep count, so a faster sweep keeps the same timing", () => {
    const { sweepsPerPeriod } = ImportedTranscriptSync;
    expect(sweepsPerPeriod(Duration.minutes(1), Duration.seconds(3))).toBe(20);
    expect(sweepsPerPeriod(Duration.minutes(5), Duration.seconds(3))).toBe(100);
    expect(sweepsPerPeriod(Duration.minutes(1), Duration.seconds(30))).toBe(2);
    expect(sweepsPerPeriod(Duration.seconds(1), Duration.seconds(3))).toBe(1);
  });
});
