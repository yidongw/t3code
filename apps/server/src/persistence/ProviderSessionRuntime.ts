import * as Arr from "effect/Array";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

import {
  AgentSessionImportSource,
  IsoDateTime,
  ProviderInstanceId,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";

import {
  PersistenceDecodeError,
  type PersistenceErrorCorrelation,
  PersistenceSqlError,
  type ProviderSessionRuntimeRepositoryError,
} from "./Errors.ts";

const PREVIOUS_PAYLOAD = `(CASE
  WHEN json_valid(provider_session_runtime.runtime_payload_json)
  THEN provider_session_runtime.runtime_payload_json
  ELSE '{}'
END)`;
const UPDATED_PAYLOAD = `(CASE
  WHEN json_type(excluded.runtime_payload_json) = 'object'
  THEN excluded.runtime_payload_json
  ELSE '{}'
END)`;

const ProviderSessionRuntimeStatus = Schema.Literals(["starting", "running", "stopped", "error"]);

export const ProviderSessionRuntime = Schema.Struct({
  threadId: ThreadId,
  providerName: Schema.String,
  providerInstanceId: Schema.NullOr(ProviderInstanceId),
  adapterKey: Schema.String,
  runtimeMode: RuntimeMode,
  status: ProviderSessionRuntimeStatus,
  lastSeenAt: IsoDateTime,
  resumeCursor: Schema.NullOr(Schema.Unknown),
  runtimePayload: Schema.NullOr(Schema.Unknown),
});
export type ProviderSessionRuntime = typeof ProviderSessionRuntime.Type;

export const GetProviderSessionRuntimeInput = Schema.Struct({ threadId: ThreadId });
export type GetProviderSessionRuntimeInput = typeof GetProviderSessionRuntimeInput.Type;

export const DeleteProviderSessionRuntimeInput = Schema.Struct({ threadId: ThreadId });
export type DeleteProviderSessionRuntimeInput = typeof DeleteProviderSessionRuntimeInput.Type;

export const RecordImportedTranscriptInput = Schema.Struct({
  threadId: ThreadId,
  source: AgentSessionImportSource,
});
export type RecordImportedTranscriptInput = typeof RecordImportedTranscriptInput.Type;

export const RecordImportedHistoryBackfillInput = Schema.Struct({
  threadId: ThreadId,
  state: Schema.Unknown,
});
export type RecordImportedHistoryBackfillInput = typeof RecordImportedHistoryBackfillInput.Type;

export interface ProviderSessionRuntimeUpsertOptions {
  readonly onConflict?: "update" | "ignore";
}

export class ProviderSessionRuntimeRepository extends Context.Service<
  ProviderSessionRuntimeRepository,
  {
    readonly upsert: (
      runtime: ProviderSessionRuntime,
      options?: ProviderSessionRuntimeUpsertOptions,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;

    readonly recordImportedTranscript: (
      input: RecordImportedTranscriptInput,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;

    /** Replaces the progress of an imported thread's history backfill. */
    readonly recordImportedHistoryBackfill: (
      input: RecordImportedHistoryBackfillInput,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;

    readonly getByThreadId: (
      input: GetProviderSessionRuntimeInput,
    ) => Effect.Effect<
      Option.Option<ProviderSessionRuntime>,
      ProviderSessionRuntimeRepositoryError
    >;

    readonly list: (options?: {
      readonly excludeStopped?: boolean;
    }) => Effect.Effect<
      ReadonlyArray<ProviderSessionRuntime>,
      ProviderSessionRuntimeRepositoryError
    >;

    readonly deleteByThreadId: (
      input: DeleteProviderSessionRuntimeInput,
    ) => Effect.Effect<void, ProviderSessionRuntimeRepositoryError>;
  }
>()("t3/persistence/ProviderSessionRuntime/ProviderSessionRuntimeRepository") {}

const ProviderSessionRuntimeDbRowSchema = ProviderSessionRuntime.mapFields(
  Struct.assign({
    resumeCursor: Schema.NullOr(Schema.fromJsonString(Schema.Unknown)),
    runtimePayload: Schema.NullOr(Schema.fromJsonString(Schema.Unknown)),
  }),
);

const ProviderSessionRuntimeRawDbRowSchema = Schema.Struct({
  threadId: Schema.String,
  providerName: Schema.Unknown,
  providerInstanceId: Schema.Unknown,
  adapterKey: Schema.Unknown,
  runtimeMode: Schema.Unknown,
  status: Schema.Unknown,
  lastSeenAt: Schema.Unknown,
  resumeCursor: Schema.Unknown,
  runtimePayload: Schema.Unknown,
});

const decodeRuntimeRow = Schema.decodeUnknownEffect(ProviderSessionRuntimeDbRowSchema);

const GetRuntimeRequestSchema = Schema.Struct({
  threadId: ThreadId,
});

const DeleteRuntimeRequestSchema = GetRuntimeRequestSchema;

const RecordImportedTranscriptRequestSchema = RecordImportedTranscriptInput.mapFields(
  Struct.assign({ source: Schema.fromJsonString(AgentSessionImportSource) }),
);

function toPersistenceSqlOrDecodeError(
  sqlOperation: string,
  decodeOperation: string,
  correlation?: PersistenceErrorCorrelation,
) {
  return (cause: unknown): ProviderSessionRuntimeRepositoryError =>
    Schema.isSchemaError(cause)
      ? PersistenceDecodeError.fromSchemaError(decodeOperation, cause, correlation)
      : new PersistenceSqlError({
          operation: sqlOperation,
          ...(correlation === undefined ? {} : { correlation }),
          cause,
        });
}

/** @public */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Runtime payloads may be stale; only the record* methods can change import records.
  const preserveImportRecords = sql.literal(`CASE
    WHEN json_type(${PREVIOUS_PAYLOAD}, '$.importedTranscripts') IS NOT NULL
      AND json_type(${PREVIOUS_PAYLOAD}, '$.importedHistoryBackfill') IS NOT NULL
    THEN json_set(${UPDATED_PAYLOAD},
      '$.importedTranscripts', json_extract(${PREVIOUS_PAYLOAD}, '$.importedTranscripts'),
      '$.importedHistoryBackfill', json_extract(${PREVIOUS_PAYLOAD}, '$.importedHistoryBackfill'))
    WHEN json_type(${PREVIOUS_PAYLOAD}, '$.importedTranscripts') IS NOT NULL
    THEN json_set(${UPDATED_PAYLOAD},
      '$.importedTranscripts', json_extract(${PREVIOUS_PAYLOAD}, '$.importedTranscripts'))
    WHEN json_type(${PREVIOUS_PAYLOAD}, '$.importedHistoryBackfill') IS NOT NULL
    THEN json_set(${UPDATED_PAYLOAD},
      '$.importedHistoryBackfill', json_extract(${PREVIOUS_PAYLOAD}, '$.importedHistoryBackfill'))
    ELSE excluded.runtime_payload_json
  END`);
  const upsertRuntimeRow = SqlSchema.void({
    Request: ProviderSessionRuntimeDbRowSchema,
    execute: (runtime) =>
      sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          provider_instance_id,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        )
        VALUES (
          ${runtime.threadId},
          ${runtime.providerName},
          ${runtime.providerInstanceId},
          ${runtime.adapterKey},
          ${runtime.runtimeMode},
          ${runtime.status},
          ${runtime.lastSeenAt},
          ${runtime.resumeCursor},
          CASE
            WHEN json_type(${runtime.runtimePayload}) = 'object'
            THEN json_remove(${runtime.runtimePayload}, '$.importedTranscripts', '$.importedHistoryBackfill')
            ELSE ${runtime.runtimePayload}
          END
        )
        ON CONFLICT (thread_id)
        DO UPDATE SET
          provider_name = excluded.provider_name,
          provider_instance_id = excluded.provider_instance_id,
          adapter_key = excluded.adapter_key,
          runtime_mode = excluded.runtime_mode,
          status = excluded.status,
          last_seen_at = excluded.last_seen_at,
          resume_cursor_json = excluded.resume_cursor_json,
          runtime_payload_json = ${preserveImportRecords}
      `,
  });

  const insertRuntimeRow = SqlSchema.void({
    Request: ProviderSessionRuntimeDbRowSchema,
    execute: (runtime) =>
      sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          provider_instance_id,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        )
        VALUES (
          ${runtime.threadId},
          ${runtime.providerName},
          ${runtime.providerInstanceId},
          ${runtime.adapterKey},
          ${runtime.runtimeMode},
          ${runtime.status},
          ${runtime.lastSeenAt},
          ${runtime.resumeCursor},
          CASE
            WHEN json_type(${runtime.runtimePayload}) = 'object'
            THEN json_remove(${runtime.runtimePayload}, '$.importedTranscripts', '$.importedHistoryBackfill')
            ELSE ${runtime.runtimePayload}
          END
        )
        ON CONFLICT (thread_id) DO NOTHING
      `,
  });

  const recordImportedTranscriptRow = SqlSchema.void({
    Request: RecordImportedTranscriptRequestSchema,
    execute: ({ threadId, source }) =>
      sql`
        WITH current_runtime AS (
          SELECT CASE
            WHEN json_valid(runtime_payload_json) THEN CASE
              WHEN json_type(runtime_payload_json) = 'object' THEN runtime_payload_json
              ELSE '{}'
            END
            ELSE '{}'
          END AS payload
          FROM provider_session_runtime
          WHERE thread_id = ${threadId}
        )
        UPDATE provider_session_runtime
        SET runtime_payload_json = (
          SELECT json_set(
            payload,
            '$.importedTranscripts',
            json((
              SELECT json_group_array(json(value))
              FROM (
                SELECT value
                FROM json_each(CASE
                  WHEN json_type(payload, '$.importedTranscripts') = 'array'
                  THEN json_extract(payload, '$.importedTranscripts')
                  ELSE '[]'
                END)
                WHERE CASE
                  WHEN type = 'object' THEN
                    json_extract(value, '$.providerInstanceId')
                      IS NOT json_extract(${source}, '$.providerInstanceId')
                    OR json_extract(value, '$.filePath') IS NOT json_extract(${source}, '$.filePath')
                  ELSE 0
                END
                UNION ALL
                SELECT ${source} AS value
              )
            ))
          )
          FROM current_runtime
        )
        WHERE thread_id = ${threadId}
      `,
  });

  const recordImportedHistoryBackfillRow = SqlSchema.void({
    Request: RecordImportedHistoryBackfillInput.mapFields(
      Struct.assign({ state: Schema.fromJsonString(Schema.Unknown) }),
    ),
    execute: ({ threadId, state }) =>
      sql`
        UPDATE provider_session_runtime
        SET runtime_payload_json = json_set(
          CASE
            WHEN json_valid(runtime_payload_json) THEN CASE
              WHEN json_type(runtime_payload_json) = 'object' THEN runtime_payload_json
              ELSE '{}'
            END
            ELSE '{}'
          END,
          '$.importedHistoryBackfill',
          json(${state})
        )
        WHERE thread_id = ${threadId}
      `,
  });

  const getRuntimeRowByThreadId = SqlSchema.findOneOption({
    Request: GetRuntimeRequestSchema,
    Result: ProviderSessionRuntimeRawDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          provider_name AS "providerName",
          provider_instance_id AS "providerInstanceId",
          adapter_key AS "adapterKey",
          runtime_mode AS "runtimeMode",
          status,
          last_seen_at AS "lastSeenAt",
          resume_cursor_json AS "resumeCursor",
          runtime_payload_json AS "runtimePayload"
        FROM provider_session_runtime
        WHERE thread_id = ${threadId}
      `,
  });

  const listRuntimeRows = SqlSchema.findAll({
    Request: Schema.Struct({ excludeStopped: Schema.Boolean }),
    Result: ProviderSessionRuntimeRawDbRowSchema,
    execute: ({ excludeStopped }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          provider_name AS "providerName",
          provider_instance_id AS "providerInstanceId",
          adapter_key AS "adapterKey",
          runtime_mode AS "runtimeMode",
          status,
          last_seen_at AS "lastSeenAt",
          resume_cursor_json AS "resumeCursor",
          runtime_payload_json AS "runtimePayload"
        FROM provider_session_runtime
        ${excludeStopped ? sql`WHERE status != 'stopped'` : sql``}
        ORDER BY last_seen_at ASC, thread_id ASC
      `,
  });

  const deleteRuntimeByThreadId = SqlSchema.void({
    Request: DeleteRuntimeRequestSchema,
    execute: ({ threadId }) =>
      sql`
        DELETE FROM provider_session_runtime
        WHERE thread_id = ${threadId}
      `,
  });

  const upsert: ProviderSessionRuntimeRepository["Service"]["upsert"] = (runtime, options) =>
    (options?.onConflict === "ignore" ? insertRuntimeRow(runtime) : upsertRuntimeRow(runtime)).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProviderSessionRuntimeRepository.upsert:query",
          "ProviderSessionRuntimeRepository.upsert:encodeRequest",
          { threadId: runtime.threadId },
        ),
      ),
    );

  const recordImportedTranscript: ProviderSessionRuntimeRepository["Service"]["recordImportedTranscript"] =
    (input) =>
      recordImportedTranscriptRow(input).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProviderSessionRuntimeRepository.recordImportedTranscript:query",
            "ProviderSessionRuntimeRepository.recordImportedTranscript:encodeRequest",
            { threadId: input.threadId },
          ),
        ),
      );

  const recordImportedHistoryBackfill: ProviderSessionRuntimeRepository["Service"]["recordImportedHistoryBackfill"] =
    (input) =>
      recordImportedHistoryBackfillRow(input).pipe(
        Effect.mapError(
          toPersistenceSqlOrDecodeError(
            "ProviderSessionRuntimeRepository.recordImportedHistoryBackfill:query",
            "ProviderSessionRuntimeRepository.recordImportedHistoryBackfill:encodeRequest",
            { threadId: input.threadId },
          ),
        ),
      );

  const getByThreadId: ProviderSessionRuntimeRepository["Service"]["getByThreadId"] = (input) =>
    getRuntimeRowByThreadId(input).pipe(
      NodeSqliteClient.readOnly,
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProviderSessionRuntimeRepository.getByThreadId:query",
          "ProviderSessionRuntimeRepository.getByThreadId:decodeRow",
          { threadId: input.threadId },
        ),
      ),
      Effect.flatMap((runtimeRowOption) =>
        Option.match(runtimeRowOption, {
          onNone: () => Effect.succeedNone,
          onSome: (row) =>
            decodeRuntimeRow(row).pipe(
              Effect.mapError((cause) =>
                PersistenceDecodeError.fromSchemaError(
                  "ProviderSessionRuntimeRepository.getByThreadId:decodeRow",
                  cause,
                  { threadId: input.threadId },
                ),
              ),
              Effect.asSome,
            ),
        }),
      ),
    );

  const list: ProviderSessionRuntimeRepository["Service"]["list"] = (options) =>
    listRuntimeRows({ excludeStopped: options?.excludeStopped === true }).pipe(
      NodeSqliteClient.readOnly,
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProviderSessionRuntimeRepository.list:query",
          "ProviderSessionRuntimeRepository.list:decodeRows",
        ),
      ),
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeRuntimeRow(row).pipe(
            Effect.asSome,
            Effect.catch((cause) =>
              Effect.logWarning("provider.session.runtime.row-skipped", {
                threadId: row.threadId,
                error: PersistenceDecodeError.fromSchemaError(
                  "ProviderSessionRuntimeRepository.list:decodeRows",
                  cause,
                  { threadId: row.threadId },
                ).message,
              }).pipe(Effect.as(Option.none<ProviderSessionRuntime>())),
            ),
          ),
        ),
      ),
      Effect.map((decoded) =>
        Arr.filterMap(decoded, (row) =>
          Option.isSome(row) ? Result.succeed(row.value) : Result.failVoid,
        ),
      ),
    );

  const deleteByThreadId: ProviderSessionRuntimeRepository["Service"]["deleteByThreadId"] = (
    input,
  ) =>
    deleteRuntimeByThreadId(input).pipe(
      Effect.mapError(
        (cause) =>
          new PersistenceSqlError({
            operation: "ProviderSessionRuntimeRepository.deleteByThreadId:query",
            correlation: { threadId: input.threadId },
            cause,
          }),
      ),
    );

  return {
    upsert,
    recordImportedTranscript,
    recordImportedHistoryBackfill,
    getByThreadId,
    list,
    deleteByThreadId,
  } satisfies ProviderSessionRuntimeRepository["Service"];
});

export const layer = Layer.effect(ProviderSessionRuntimeRepository, make);
