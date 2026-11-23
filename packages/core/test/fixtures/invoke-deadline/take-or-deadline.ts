import { createAsyncLogic, setup, TimeoutError } from 'xstate';

/** Your own queue. `take` must stop waiting when its abort signal aborts. */
export interface MessageQueue {
  take(signal: AbortSignal): Promise<string>;
}

export function createRetryMachine(messages: MessageQueue) {
  // The logic owns the deadline. After four seconds it aborts `signal`,
  // which withdraws the pending take, and then errors with a TimeoutError.
  const takeMessage = createAsyncLogic({
    timeout: 4_000,
    run: ({ signal }) => messages.take(signal)
  });

  return setup({ actors: { takeMessage } }).createMachine({
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'takeMessage',
          // The message is only a wake-up signal, so onDone ignores it.
          onDone: { target: 'retryNow' },
          onError: ({ event }) => ({
            target:
              event.error instanceof TimeoutError ? 'retryLater' : 'failed'
          })
        }
      },
      retryNow: {},
      retryLater: {},
      failed: {}
    }
  });
}
