import { OrchestrationV2TurnItem, RunId, ThreadId, TurnItemId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class TurnItemPositionStoreError extends Schema.TaggedError<TurnItemPositionStoreError>()(
  "TurnItemPositionStoreError",
  {
    threadId: ThreadId,
    turnItemId: TurnItemId,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isTurnItemPositionStoreError = Schema.is(TurnItemPositionStoreError);

/** Each run's items take one band of positions; runless items before every run take band 0. */
export const RUN_BAND_SIZE = 1_000_000;

export interface TurnItemPositionStoreV2Shape {
  readonly allocate: (input: {
    readonly threadId: ThreadId;
    readonly turnItemId: TurnItemId;
    readonly runId: RunId | null;
    readonly runOrdinal?: number;
  }) => Effect.Effect<number, TurnItemPositionStoreError>;
  readonly normalize: (
    item: OrchestrationV2TurnItem,
    runOrdinal?: number,
  ) => Effect.Effect<OrchestrationV2TurnItem, TurnItemPositionStoreError>;
  /** The highest position held in a run's band, or among runless items before every run. */
  readonly maxInBand: (input: {
    readonly threadId: ThreadId;
    readonly runOrdinal: number;
  }) => Effect.Effect<number | null, TurnItemPositionStoreError>;
  /** Moves items to given positions, all or none; a position held by another item fails. */
  readonly place: (input: {
    readonly threadId: ThreadId;
    readonly positions: ReadonlyArray<{
      readonly turnItemId: TurnItemId;
      readonly ordinal: number;
    }>;
  }) => Effect.Effect<void, TurnItemPositionStoreError>;
}

export class TurnItemPositionStoreV2 extends Context.Service<
  TurnItemPositionStoreV2,
  TurnItemPositionStoreV2Shape
>()("t3/orchestration-v2/TurnItemPositionStore/TurnItemPositionStoreV2") {}

export const layer: Layer.Layer<TurnItemPositionStoreV2, never, SqlClient.SqlClient> = Layer.effect(
  TurnItemPositionStoreV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const allocate: TurnItemPositionStoreV2Shape["allocate"] =
      /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
      Effect.fnUntraced(
        function* ({ threadId, turnItemId, runId, runOrdinal: suppliedRunOrdinal }) {
          const existing = yield* sql<{ readonly ordinal: number }>`
          SELECT ordinal
          FROM orchestration_v2_turn_item_positions
          WHERE thread_id = ${threadId} AND turn_item_id = ${turnItemId}
          LIMIT 1
        `;
          if (existing[0] !== undefined) return existing[0].ordinal;

          const runRows =
            runId === null
              ? []
              : yield* sql<{ readonly ordinal: number }>`
                SELECT ordinal
                FROM orchestration_v2_projection_runs
                WHERE thread_id = ${threadId} AND run_id = ${runId}
                LIMIT 1
              `;
          const runOrdinal = suppliedRunOrdinal ?? runRows[0]?.ordinal ?? null;
          const lowerBound = runOrdinal === null ? 0 : runOrdinal * RUN_BAND_SIZE;
          const upperBound = lowerBound + RUN_BAND_SIZE - 1;
          yield* sql`
          INSERT INTO orchestration_v2_turn_item_positions (thread_id, turn_item_id, ordinal)
          SELECT
            ${threadId},
            ${turnItemId},
            COALESCE(MAX(ordinal), ${lowerBound}) + 1
          FROM orchestration_v2_turn_item_positions
          WHERE thread_id = ${threadId}
            AND ordinal >= ${lowerBound}
            AND ordinal <= ${upperBound}
          ON CONFLICT(thread_id, turn_item_id) DO NOTHING
        `;
          const rows = yield* sql<{ readonly ordinal: number }>`
          SELECT ordinal
          FROM orchestration_v2_turn_item_positions
          WHERE thread_id = ${threadId} AND turn_item_id = ${turnItemId}
          LIMIT 1
        `;
          const ordinal = rows[0]?.ordinal;
          if (ordinal === undefined) {
            return yield* new TurnItemPositionStoreError({
              threadId,
              turnItemId,
              cause: "Position allocation returned no row.",
            });
          }
          return ordinal;
        },
        (effect, { threadId, turnItemId }) =>
          effect.pipe(
            Effect.mapError((cause) =>
              isTurnItemPositionStoreError(cause)
                ? cause
                : new TurnItemPositionStoreError({ threadId, turnItemId, cause }),
            ),
          ),
      );

    const maxInBand: TurnItemPositionStoreV2Shape["maxInBand"] = Effect.fn(
      "TurnItemPositionStore.maxInBand",
    )(function* ({ threadId, runOrdinal }) {
      const lowerBound = runOrdinal * RUN_BAND_SIZE;
      const rows = yield* sql<{ readonly ordinal: number | null }>`
        SELECT MAX(ordinal) AS ordinal
        FROM orchestration_v2_turn_item_positions
        WHERE thread_id = ${threadId}
          AND ordinal >= ${lowerBound}
          AND ordinal < ${lowerBound + RUN_BAND_SIZE}
      `.pipe(
        Effect.mapError(
          (cause) =>
            new TurnItemPositionStoreError({
              threadId,
              turnItemId: TurnItemId.make(""),
              cause,
            }),
        ),
      );
      return rows[0]?.ordinal ?? null;
    });

    const place: TurnItemPositionStoreV2Shape["place"] = Effect.fn("TurnItemPositionStore.place")(
      function* ({ threadId, positions }) {
        yield* sql
          .withTransaction(
            Effect.forEach(
              positions,
              ({ turnItemId, ordinal }) => sql`
                INSERT INTO orchestration_v2_turn_item_positions (thread_id, turn_item_id, ordinal)
                VALUES (${threadId}, ${turnItemId}, ${ordinal})
                ON CONFLICT(thread_id, turn_item_id) DO UPDATE SET ordinal = excluded.ordinal
              `,
              { discard: true },
            ),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new TurnItemPositionStoreError({
                  threadId,
                  turnItemId: positions[0]?.turnItemId ?? TurnItemId.make(""),
                  cause,
                }),
            ),
          );
      },
    );

    return TurnItemPositionStoreV2.of({
      allocate,
      maxInBand,
      place,
      // Per-event normalization shares the enclosing orchestration span.
      normalize: Effect.fnUntraced(function* (item, runOrdinal) {
        return yield* allocate({
          threadId: item.threadId,
          turnItemId: item.id,
          runId: item.runId,
          ...(runOrdinal === undefined ? {} : { runOrdinal }),
        }).pipe(Effect.map((ordinal) => (item.ordinal === ordinal ? item : { ...item, ordinal })));
      }),
    });
  }),
);
