import type { Result } from "../result";
import type {
  ActorId,
  ActorDiagnostics,
  ActorRef,
  Lock,
  MainToWorkerMessage,
} from "./types";

/**
 * Factory for ActorRef — user-facing handle to a spawned actor.
 * Delegates all operations to supervisor internals via injected callbacks.
 */
export function createActorRef(options: {
  readonly id: ActorId;
  readonly sendToWorker: (msg: MainToWorkerMessage) => void;
  readonly subscribeToMessages: (fn: (msg: unknown) => void) => () => void;
  readonly terminate: () => void;
  readonly readBox: (lock: Lock) => Uint8Array;
  readonly doneBox: (lock: Lock) => void;
  readonly getDiagnostics: () => Result<ActorDiagnostics, string>;
  readonly linkTo: (otherId: string) => void;
  readonly monitor: (otherId: string) => void;
}): ActorRef {
  const {
    id,
    sendToWorker,
    subscribeToMessages,
    terminate,
    readBox,
    doneBox,
    getDiagnostics,
    linkTo,
    monitor,
  } = options;

  const spawn = (msg: unknown): void => {
    sendToWorker({ type: "SPAWN", data: msg });
  };

  const subscribe = (fn: (msg: unknown) => void): (() => void) => {
    return subscribeToMessages(fn);
  };

  const read = (lock: Lock): Uint8Array => {
    return readBox(lock);
  };

  const done = (lock: Lock): void => {
    doneBox(lock);
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
    done,
    getDiagnostics,
    link,
    monitor: monitorActor,
  });
}