import {
  makeThreadTriageOutboxStore,
  ThreadTriageOutboxStorageError,
} from "@t3tools/client-runtime/platform";
import * as Effect from "effect/Effect";

const STORAGE_KEY = "t3code:thread-triage-outbox";

/** Kept out of the environment cache so clearing cached data keeps unsynced settles and snoozes. */
export const browserThreadTriageOutboxStore = makeThreadTriageOutboxStore({
  read: Effect.try({
    try: () => window.localStorage.getItem(STORAGE_KEY),
    catch: (cause) => new ThreadTriageOutboxStorageError({ cause }),
  }),
  write: (value) =>
    Effect.try({
      try: () => window.localStorage.setItem(STORAGE_KEY, value),
      catch: (cause) => new ThreadTriageOutboxStorageError({ cause }),
    }),
});
