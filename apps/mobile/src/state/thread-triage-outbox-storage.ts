import {
  makeThreadTriageOutboxStore,
  ThreadTriageOutboxStorageError,
} from "@t3tools/client-runtime/platform";
import * as Effect from "effect/Effect";

import { writeFileAtomically } from "../lib/atomic-file";

async function getStorageFile() {
  const { File, Paths } = await import("expo-file-system");
  return new File(Paths.document, "thread-triage-outbox.json");
}

/** Lives outside the client cache so clearing cached data keeps unsynced settles and snoozes. */
export const mobileThreadTriageOutboxStore = makeThreadTriageOutboxStore({
  read: Effect.tryPromise({
    try: async () => {
      const file = await getStorageFile();
      return file.exists ? await file.text() : null;
    },
    catch: (cause) => new ThreadTriageOutboxStorageError({ cause }),
  }),
  write: (value) =>
    Effect.tryPromise({
      try: async () => writeFileAtomically(await getStorageFile(), value),
      catch: (cause) => new ThreadTriageOutboxStorageError({ cause }),
    }),
});
