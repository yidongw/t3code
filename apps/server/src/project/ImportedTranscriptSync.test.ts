import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
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
import * as Path from "effect/Path";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
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

const setup = Effect.fn(function* (options?: { readonly maxSyncBytes?: number }) {
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
  yield* writeTranscript(filePath, imported);
  const releases: Array<ProviderSessionId> = [];

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
      ...ImportedTranscriptSync.importedMessageEvents({
        threadId,
        prefix: ImportedTranscriptSync.IMPORT_EVENT_PREFIX,
        messageKey: "000000",
        itemKey: "000000",
        ordinal: 1,
        message: { role: "user", text: "hi", createdAt: at("10:00:00") },
      }),
      ...ImportedTranscriptSync.importedMessageEvents({
        threadId,
        prefix: ImportedTranscriptSync.IMPORT_EVENT_PREFIX,
        messageKey: "000001",
        itemKey: "000001",
        ordinal: 2,
        message: { role: "assistant", text: "hello", createdAt: at("10:00:01") },
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

  const context = yield* Layer.build(
    ImportedTranscriptSync.layerWith(options ?? {}).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(EventSink.EventSinkV2, sink),
          Layer.succeed(
            ProjectionStore.ProjectionStoreV2,
            yield* ProjectionStore.ProjectionStoreV2,
          ),
          Layer.succeed(ProviderSessionRuntime.ProviderSessionRuntimeRepository, runtimes),
          Layer.succeed(FileSystem.FileSystem, fileSystem),
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
  );
  const sync = Context.get(context, ImportedTranscriptSync.ImportedTranscriptSync);
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
    sink,
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
