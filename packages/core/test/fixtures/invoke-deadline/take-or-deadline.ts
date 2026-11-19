import { createAsyncLogic, setup, TimeoutError } from 'xstate';

/** Your own queue. `take` must stop waiting when its signal aborts. */
export interface SignalQueue {
  take(signal: AbortSignal): Promise<string>;
}

export function createRetryMachine(signals: SignalQueue) {
  // The logic owns the deadline. After four seconds it aborts `signal`,
  // which withdraws the pending take, and then errors with a TimeoutError.
  const takeSignal = createAsyncLogic({
    timeout: 4_000,
    run: ({ signal }) => signals.take(signal)
  });

  return setup({ actors: { takeSignal } }).createMachine({
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'takeSignal',
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
