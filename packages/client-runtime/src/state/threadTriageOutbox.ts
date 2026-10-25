import {
  CommandId,
  type EnvironmentId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ThreadShell,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { AsyncResult, Atom, type AtomRegistry } from "effect/reactivity";

import {
  type PendingThreadTriage,
  type ThreadTriageKey,
  type ThreadTriageOutboxDocument,
  sameThreadTriage,
  ThreadTriageOutboxStore,
  type ThreadTriageOutboxStorageError,
} from "../platform/threadTriageOutboxStore.ts";
import { EnvironmentRpcUnavailableError, isRpcClientError } from "../rpc/client.ts";
import type { EnvironmentShellStatus } from "./shell.ts";
import { type AtomCommand, type AtomCommandResult, createRuntimeCommand } from "./runtime.ts";

type SettleOrSnoozeInput = {
  readonly threadId: ThreadId;
  readonly commandId?: CommandId;
  readonly snoozedUntil?: string;
  readonly snapshotSequence?: number;
};
type TriageResult = { readonly sequence: number; readonly pendingSync?: true };
type Command<I, E> = AtomCommand<
  { readonly environmentId: EnvironmentId; readonly input: I },
  TriageResult,
  E
>;

type StoreOperation =
  | { readonly _tag: "load" }
  | { readonly _tag: "put"; readonly entry: Omit<PendingThreadTriage, "commandId"> }
  | { readonly _tag: "remove"; readonly entry: PendingThreadTriage }
  | {
      readonly _tag: "setOperable";
      readonly environmentId: EnvironmentId;
      readonly operable: boolean;
    };

const EMPTY_DOCUMENT: ThreadTriageOutboxDocument = { entries: [], operableEnvironmentIds: [] };

const isUnavailable = Schema.is(EnvironmentRpcUnavailableError);

function isOffline(cause: Cause.Cause<unknown>) {
  return cause.reasons.some((reason) => Cause.isFailReason(reason) && isUnavailable(reason.error));
}

/** Failures that say nothing about whether the server would accept the change. */
function isTransient(cause: Cause.Cause<unknown>) {
  return (
    Cause.hasInterruptsOnly(cause) ||
    cause.reasons.some(
      (reason) =>
        !Cause.isFailReason(reason) ||
        isUnavailable(reason.error) ||
        isRpcClientError(reason.error),
    )
  );
}

export function createThreadTriageOutbox<R, E>(input: {
  readonly runtime: Atom.AtomRuntime<Crypto.Crypto | R, E>;
  readonly sourceSnapshotAtom: (
    environmentId: EnvironmentId,
  ) => Atom.Atom<OrchestrationV2ShellSnapshot | null>;
  /** Replay waits for a synchronized shell; without one, nothing replays. */
  readonly shellStatusAtom?: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellStatus>;
  /** Whether the loaded session can change threads; null until it loads. */
  readonly operateScopeAtom?: (environmentId: EnvironmentId) => Atom.Atom<boolean | null>;
  readonly applySettle: (
    thread: OrchestrationV2ThreadShell,
    queuedAt: DateTime.Utc,
  ) => OrchestrationV2ThreadShell;
  readonly applySnooze: (
    thread: OrchestrationV2ThreadShell,
    snoozedUntil: string,
    queuedAt: DateTime.Utc,
  ) => OrchestrationV2ThreadShell;
  /** Sends the queued action with its command id and snapshot sequence. */
  readonly replay: (
    registry: AtomRegistry.AtomRegistry,
    entry: PendingThreadTriage,
  ) => Promise<AtomCommandResult<unknown, unknown>>;
}) {
  const store = createRuntimeCommand(input.runtime, {
    label: "environment-data:thread-triage-outbox",
    execute: Effect.fn("ThreadTriageOutbox.store")(function* (operation: StoreOperation) {
      const outbox = yield* ThreadTriageOutboxStore;
      switch (operation._tag) {
        case "load":
          return yield* outbox.load.pipe(
            Effect.catch((error) =>
              Effect.logWarning("Could not load offline thread changes.", {
                cause: String(error.cause),
              }).pipe(Effect.as(EMPTY_DOCUMENT)),
            ),
          );
        case "put": {
          const crypto = yield* Crypto.Crypto;
          const entry = {
            ...operation.entry,
            commandId: CommandId.make(yield* crypto.randomUUIDv4().pipe(Effect.orDie)),
          };
          yield* outbox.put(entry).pipe(
            Effect.tapError((error) =>
              Effect.logWarning("Could not save an offline thread change.", {
                cause: String(error.cause),
              }),
            ),
          );
          return { ...EMPTY_DOCUMENT, entries: [entry] };
        }
        case "remove":
          yield* outbox.remove(operation.entry).pipe(
            Effect.tapError((error) =>
              Effect.logWarning("Could not remove an offline thread change.", {
                cause: String(error.cause),
              }),
            ),
          );
          return EMPTY_DOCUMENT;
        case "setOperable":
          yield* outbox.setOperable(operation.environmentId, operation.operable).pipe(
            Effect.catch((error) =>
              Effect.logWarning("Could not remember thread permissions.", {
                cause: String(error.cause),
              }),
            ),
          );
          return EMPTY_DOCUMENT;
      }
    }),
  });

  const documentAtom = Atom.make<ThreadTriageOutboxDocument | null>(null).pipe(Atom.keepAlive);
  let loading: Promise<ThreadTriageOutboxDocument> | undefined;
  const ensureLoaded = (registry: AtomRegistry.AtomRegistry) => {
    const loaded = registry.get(documentAtom);
    if (loaded !== null) return Promise.resolve(loaded);
    loading ??= store.run(registry, { _tag: "load" }).then((result) => {
      const current =
        registry.get(documentAtom) ??
        (AsyncResult.isSuccess(result) ? result.value : EMPTY_DOCUMENT);
      registry.set(documentAtom, current);
      return current;
    });
    return loading;
  };
  const loadedDocumentAtom = Atom.make((get) => {
    const loaded = get(documentAtom);
    if (loaded === null) void ensureLoaded(get.registry);
    return loaded;
  });

  const entriesAtom = Atom.make((get) => get(loadedDocumentAtom)?.entries ?? []);
  const environmentEntriesAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) => get(entriesAtom).filter((entry) => entry.environmentId === environmentId)),
  );

  const replaying = new Set<string>();
  const keyOf = (entry: ThreadTriageKey) => JSON.stringify([entry.environmentId, entry.threadId]);
  const pendingFor = (registry: AtomRegistry.AtomRegistry, key: ThreadTriageKey) =>
    registry.get(documentAtom)?.entries.find((entry) => sameThreadTriage(entry, key));
  const setLocal = (
    registry: AtomRegistry.AtomRegistry,
    transform: (entries: ReadonlyArray<PendingThreadTriage>) => ReadonlyArray<PendingThreadTriage>,
  ) => {
    const document = registry.get(documentAtom) ?? EMPTY_DOCUMENT;
    registry.set(documentAtom, { ...document, entries: transform(document.entries) });
  };
  /**
   * Hides the entry even if storage keeps it: a restart then replays it with the same command
   * id, which the server deduplicates or rejects. Cancelling must check the result instead.
   */
  const forget = async (registry: AtomRegistry.AtomRegistry, entry: PendingThreadTriage) => {
    const removed = await store.run(registry, { _tag: "remove", entry });
    setLocal(registry, (entries) => entries.filter((current) => current !== entry));
    return removed;
  };

  const replayingEnvironments = new Set<EnvironmentId>();
  const replayAgain = new Set<EnvironmentId>();
  const replay = async (registry: AtomRegistry.AtomRegistry, environmentId: EnvironmentId) => {
    if (replayingEnvironments.has(environmentId)) {
      replayAgain.add(environmentId);
      return;
    }
    replayingEnvironments.add(environmentId);
    try {
      await replayEntries(registry, environmentId);
    } finally {
      replayingEnvironments.delete(environmentId);
    }
    if (replayAgain.delete(environmentId) && input.shellStatusAtom !== undefined) {
      if (registry.get(input.shellStatusAtom(environmentId)) === "live") {
        await replay(registry, environmentId);
      }
    }
  };
  const replayEntries = async (
    registry: AtomRegistry.AtomRegistry,
    environmentId: EnvironmentId,
  ) => {
    for (const entry of registry.get(environmentEntriesAtom(environmentId))) {
      const key = keyOf(entry);
      if (replaying.has(key)) continue;
      if (
        entry.action.type === "snooze" &&
        !(Date.parse(entry.action.snoozedUntil) > DateTime.toEpochMillis(DateTime.nowUnsafe()))
      ) {
        await forget(registry, entry);
        continue;
      }
      replaying.add(key);
      try {
        const result = await input.replay(registry, entry);
        if (AsyncResult.isSuccess(result) || !isTransient(result.cause)) {
          await forget(registry, entry);
        } else {
          return;
        }
      } finally {
        replaying.delete(key);
      }
    }
  };

  const replayAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) => {
      const status = input.shellStatusAtom ? get(input.shellStatusAtom(environmentId)) : "empty";
      const entries = get(environmentEntriesAtom(environmentId));
      if (status === "live" && entries.length > 0) void replay(get.registry, environmentId);
    }),
  );

  const snapshotAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) => {
      get.mount(replayAtom(environmentId));
      const snapshot = get(input.sourceSnapshotAtom(environmentId));
      const entries = get(environmentEntriesAtom(environmentId));
      if (snapshot === null || entries.length === 0) return snapshot;
      const byThread = new Map(entries.map((entry) => [entry.threadId, entry]));
      return {
        ...snapshot,
        threads: snapshot.threads.map((thread) => {
          const entry = byThread.get(thread.id);
          if (entry === undefined) return thread;
          return entry.action.type === "settle"
            ? input.applySettle(thread, entry.queuedAt)
            : input.applySnooze(thread, entry.action.snoozedUntil, entry.queuedAt);
        }),
      };
    }),
  );

  const pendingSyncAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make(
      (get): ReadonlySet<ThreadId> =>
        new Set(get(environmentEntriesAtom(environmentId)).map((entry) => entry.threadId)),
    ),
  );

  /** Queues the action locally when the environment is not connected. */
  function queueWhenOffline<I extends SettleOrSnoozeInput, CE>(
    command: Command<I, CE>,
    action: (input: I) => PendingThreadTriage["action"],
  ): Command<I, CE> {
    return {
      label: command.label,
      run: async (registry, target) => {
        const key = { environmentId: target.environmentId, threadId: target.input.threadId };
        const previous = pendingFor(registry, key);
        // Started before awaiting so the optimistic change shows in the same frame.
        const running = command.run(registry, target);
        if (previous !== undefined && !replaying.has(keyOf(previous))) {
          await forget(registry, previous);
        }
        const result = await running;
        if (AsyncResult.isSuccess(result) || !isOffline(result.cause)) return result;
        await ensureLoaded(registry);
        const snapshot = registry.get(input.sourceSnapshotAtom(target.environmentId));
        const thread = snapshot?.threads.find((candidate) => candidate.id === key.threadId);
        if (snapshot == null || thread === undefined) return result;
        const queued = await store.run(registry, {
          _tag: "put",
          entry: {
            ...key,
            action: action(target.input),
            queuedAt: DateTime.nowUnsafe(),
            snapshotSequence: snapshot.snapshotSequence,
          },
        });
        if (!AsyncResult.isSuccess(queued)) return result;
        const entry = queued.value.entries[0]!;
        setLocal(registry, (entries) => [
          ...entries.filter((current) => !sameThreadTriage(current, entry)),
          entry,
        ]);
        return AsyncResult.success({ sequence: snapshot.snapshotSequence, pendingSync: true });
      },
    };
  }

  /** Undo before sync cancels the queued action instead of asking the server. */
  function cancelQueued<I extends { readonly threadId: ThreadId }, A, CE>(
    command: AtomCommand<{ readonly environmentId: EnvironmentId; readonly input: I }, A, CE>,
    cancels: PendingThreadTriage["action"]["type"],
    cancelledValue: A,
  ): AtomCommand<
    { readonly environmentId: EnvironmentId; readonly input: I },
    A,
    CE | ThreadTriageOutboxStorageError | E
  > {
    return {
      label: command.label,
      run: async (registry, target) => {
        const pending = pendingFor(registry, {
          environmentId: target.environmentId,
          threadId: target.input.threadId,
        });
        if (
          pending === undefined ||
          pending.action.type !== cancels ||
          replaying.has(keyOf(pending))
        ) {
          return command.run(registry, target);
        }
        // A cancel that stays on disk would come back and replay after a restart.
        const removed = await store.run(registry, { _tag: "remove", entry: pending });
        if (!AsyncResult.isSuccess(removed)) return AsyncResult.failure(removed.cause);
        setLocal(registry, (entries) => entries.filter((current) => current !== pending));
        return AsyncResult.success(cancelledValue);
      },
    };
  }

  /**
   * Settle and snooze stay available offline only where the last loaded session could
   * change threads. A loaded session always decides, and replay is checked again by the server.
   */
  const offlineTriagePermittedAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) => {
      const known = input.operateScopeAtom ? get(input.operateScopeAtom(environmentId)) : null;
      const document = get(loadedDocumentAtom);
      const remembered = document?.operableEnvironmentIds.includes(environmentId) ?? false;
      if (known === null) return remembered;
      if (document !== null && known !== remembered) {
        const registry = get.registry;
        registry.set(documentAtom, {
          ...document,
          operableEnvironmentIds: [
            ...document.operableEnvironmentIds.filter((id) => id !== environmentId),
            ...(known ? [environmentId] : []),
          ],
        });
        void store.run(registry, { _tag: "setOperable", environmentId, operable: known });
      }
      return known;
    }),
  );

  return {
    snapshotAtom,
    pendingSyncAtom,
    offlineTriagePermittedAtom,
    queueWhenOffline,
    cancelQueued,
  };
}
