import { createAsyncLogic, setup } from 'xstate';

type Reservation = { id: string; job: string };

/** Your broker. A reservation hides the job until it is committed or expires. */
export interface JobQueue {
  reserve(signal: AbortSignal): Promise<Reservation>;
  commit(id: string): Promise<void>;
}

export function createJobMachine(
  jobs: JobQueue,
  work: (job: string) => Promise<void>
) {
  const reserveJob = createAsyncLogic({
    run: ({ signal }): Promise<Reservation> => jobs.reserve(signal)
  });
  const runJob = createAsyncLogic({
    run: ({ input }: { input: Reservation }) => work(input.job)
  });
  const commitJob = createAsyncLogic({
    run: ({ input }: { input: Reservation }) => jobs.commit(input.id)
  });

  return setup({ actors: { reserveJob, runJob, commitJob } }).createMachine({
    context: { reservation: null as Reservation | null },
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'reserveJob',
          onDone: ({ event }) => ({
            target: 'working',
            context: { reservation: event.output }
          })
        },
        after: { 4_000: { target: 'idle' } }
      },
      // A failure in either step leaves the reservation to expire.
      working: {
        invoke: {
          src: 'runJob',
          input: ({ context }) => context.reservation!,
          onDone: { target: 'committing' },
          onError: { target: 'idle' }
        }
      },
      committing: {
        invoke: {
          src: 'commitJob',
          input: ({ context }) => context.reservation!,
          onDone: { target: 'done' },
          onError: { target: 'idle' }
        }
      },
      done: {},
      idle: {}
    }
  });
}
