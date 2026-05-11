import type { Result } from "../result";
import type {
  ActorId,
  ActorDiagnostics,
  ActorRef,
  Lock,
  MainToWorkerMessage,
  Message,
  ChainableReader,
} from "./types";

/**
 * Factory for ActorRef — user-facing handle to a spawned actor.
 * Delegates all operations to supervisor internals via injected callbacks.
 */
export function createActorRef(options: {
  readonly id: ActorId;
  readonly sendToWorker: (msg: MainToWorkerMessage) => void;
  readonly subscribeToMessages: (fn: (msg: Message) => void) => () => void;
  readonly terminate: () => void;
  readonly readFromBox: (msg: Message) => ChainableReader | null;
  readonly releaseBox: (handle: Lock) => void;
  readonly getDiagnostics: () => Result<ActorDiagnostics, string>;
  readonly linkTo: (otherId: string) => void;
  readonly monitor: (otherId: string) => void;
}): ActorRef {
  const {
    id,
    sendToWorker,
    subscribeToMessages,
    terminate,
    readFromBox,
    releaseBox,
    getDiagnostics,
    linkTo,
    monitor,
  } = options;

  const spawn = (msg: unknown): void => {
    sendToWorker({ type: "SPAWN", data: msg });
  };

  const subscribe = (fn: (msg: Message) => void): (() => void) => {
    return subscribeToMessages(fn);
  };

  const read = (msg: Message): ChainableReader | null => {
    return readFromBox(msg);
  };

  const release = (handle: Lock): void => {
    releaseBox(handle);
  };

  const link = (other: ActorRef): void => {
    linkTo(other.id);
  };

  const monitorActor = (other: ActorRef): void => {
    monitor(other.id);
  };

  return Object.freeze({
    id,
    spawn,
    subscribe,
    terminate,
    read,
    release,
    getDiagnostics,
    link,
    monitor: monitorActor,
  });
}
