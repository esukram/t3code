import { CommandId, EnvironmentId, NonNegativeInt, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

export const PendingThreadTriage = Schema.Struct({
  environmentId: EnvironmentId,
  threadId: ThreadId,
  commandId: CommandId,
  action: Schema.Union([
    Schema.Struct({ type: Schema.Literal("settle") }),
    Schema.Struct({ type: Schema.Literal("snooze"), snoozedUntil: Schema.String }),
  ]),
  queuedAt: Schema.DateTimeUtcFromString,
  /** Shell sequence the action was taken against; the server rejects it after newer changes. */
  snapshotSequence: NonNegativeInt,
});
export type PendingThreadTriage = typeof PendingThreadTriage.Type;

const ThreadTriageOutboxDocument = Schema.Struct({
  entries: Schema.Array(PendingThreadTriage),
  /** Environments whose last loaded session could change threads. */
  operableEnvironmentIds: Schema.Array(EnvironmentId),
});
export type ThreadTriageOutboxDocument = typeof ThreadTriageOutboxDocument.Type;

const EMPTY_DOCUMENT: ThreadTriageOutboxDocument = { entries: [], operableEnvironmentIds: [] };
const StoredDocument = Schema.fromJsonString(ThreadTriageOutboxDocument);
const decodeStored = Schema.decodeUnknownEffect(StoredDocument);
const encodeStored = Schema.encodeEffect(StoredDocument);

export class ThreadTriageOutboxStorageError extends Schema.TaggedError<ThreadTriageOutboxStorageError>()(
  "ThreadTriageOutboxStorageError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not save the change for later sync.";
  }
}

export type ThreadTriageKey = Pick<PendingThreadTriage, "environmentId" | "threadId">;

export const sameThreadTriage = (a: ThreadTriageKey, b: ThreadTriageKey) =>
  a.environmentId === b.environmentId && a.threadId === b.threadId;

/**
 * Settle and snooze taken while an environment is offline, kept apart from the environment
 * cache so clearing the cache does not drop them. Removing the environment does.
 */
export interface ThreadTriageOutboxStoreShape {
  readonly load: Effect.Effect<ThreadTriageOutboxDocument, ThreadTriageOutboxStorageError>;
  /** Replaces any entry for the same thread. */
  readonly put: (entry: PendingThreadTriage) => Effect.Effect<void, ThreadTriageOutboxStorageError>;
  /** Only removes the entry if it still has this command id. */
  readonly remove: (
    entry: ThreadTriageKey & Pick<PendingThreadTriage, "commandId">,
  ) => Effect.Effect<void, ThreadTriageOutboxStorageError>;
  readonly setOperable: (
    environmentId: EnvironmentId,
    operable: boolean,
  ) => Effect.Effect<void, ThreadTriageOutboxStorageError>;
  readonly clearEnvironment: (
    environmentId: EnvironmentId,
  ) => Effect.Effect<void, ThreadTriageOutboxStorageError>;
}

export class ThreadTriageOutboxStore extends Context.Reference<ThreadTriageOutboxStoreShape>(
  "@t3tools/client-runtime/state/ThreadTriageOutboxStore",
  {
    defaultValue: () => {
      let value: string | null = null;
      return makeThreadTriageOutboxStore({
        read: Effect.sync(() => value),
        write: (next) =>
          Effect.sync(() => {
            value = next;
          }),
      });
    },
  },
) {}

export function makeThreadTriageOutboxStore(storage: {
  readonly read: Effect.Effect<string | null, ThreadTriageOutboxStorageError>;
  readonly write: (value: string) => Effect.Effect<void, ThreadTriageOutboxStorageError>;
}): ThreadTriageOutboxStoreShape {
  const lock = Semaphore.makeUnsafe(1);
  const load = storage.read.pipe(
    Effect.flatMap((raw) =>
      raw === null
        ? Effect.succeed(EMPTY_DOCUMENT)
        : decodeStored(raw).pipe(
            Effect.catch((cause) =>
              Effect.logWarning("Discarding unreadable offline thread changes.", {
                cause: String(cause),
              }).pipe(Effect.as(EMPTY_DOCUMENT)),
            ),
          ),
    ),
  );
  const update = (
    transform: (document: ThreadTriageOutboxDocument) => ThreadTriageOutboxDocument,
  ) =>
    lock.withPermits(1)(
      load.pipe(
        Effect.flatMap((document) =>
          encodeStored(transform(document)).pipe(
            Effect.mapError((cause) => new ThreadTriageOutboxStorageError({ cause })),
          ),
        ),
        Effect.flatMap(storage.write),
      ),
    );
  const updateEntries = (
    transform: (entries: ReadonlyArray<PendingThreadTriage>) => ReadonlyArray<PendingThreadTriage>,
  ) => update((document) => ({ ...document, entries: transform(document.entries) }));
  return {
    load: lock.withPermits(1)(load),
    put: (entry) =>
      updateEntries((entries) => [
        ...entries.filter((current) => !sameThreadTriage(current, entry)),
        entry,
      ]),
    remove: (entry) =>
      updateEntries((entries) =>
        entries.filter(
          (current) => !sameThreadTriage(current, entry) || current.commandId !== entry.commandId,
        ),
      ),
    setOperable: (environmentId, operable) =>
      update((document) => ({
        ...document,
        operableEnvironmentIds: [
          ...document.operableEnvironmentIds.filter((id) => id !== environmentId),
          ...(operable ? [environmentId] : []),
        ],
      })),
    clearEnvironment: (environmentId) =>
      update((document) => ({
        entries: document.entries.filter((entry) => entry.environmentId !== environmentId),
        operableEnvironmentIds: document.operableEnvironmentIds.filter(
          (id) => id !== environmentId,
        ),
      })),
  };
}
