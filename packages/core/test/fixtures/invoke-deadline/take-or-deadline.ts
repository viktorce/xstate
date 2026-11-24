import { createAsyncLogic, setup, TimeoutError } from 'xstate';

/**
 * Your own queue. When its abort signal aborts, `take` must withdraw its
 * pending request synchronously, in the abort listener.
 */
export interface MessageQueue {
  take(signal: AbortSignal): Promise<string>;
}

export function createRetryMachine(messages: MessageQueue) {
  // The logic owns the deadline. After four seconds it aborts `signal`,
  // which withdraws the pending take, and then errors with a TimeoutError.
  // The return type gives `onDone` a typed `event.output`.
  const takeMessage = createAsyncLogic({
    timeout: 4_000,
    run: ({ signal }): Promise<string> => messages.take(signal)
  });

  return setup({ actors: { takeMessage } }).createMachine({
    context: { message: null as string | null },
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'takeMessage',
          onDone: ({ event }) => ({
            target: 'retryNow',
            context: { message: event.output }
          }),
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
