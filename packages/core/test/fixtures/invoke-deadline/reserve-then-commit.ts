import { createAsyncLogic, setup } from 'xstate';

type Job = { id: string; payload: string };
type Reservation = { receipt: string; job: Job };

/** Your broker. A reservation hides the job until it is committed or expires. */
export interface JobQueue {
  reserve(signal: AbortSignal): Promise<Reservation>;
  commit(receipt: string): Promise<void>;
}

export function createJobMachine(
  jobs: JobQueue,
  work: (job: Job) => Promise<void>
) {
  const reserveJob = createAsyncLogic({
    run: ({ signal }): Promise<Reservation> => jobs.reserve(signal)
  });
  const runJob = createAsyncLogic({
    run: ({ input }: { input: Reservation }) => work(input.job)
  });
  const commitJob = createAsyncLogic({
    run: ({ input }: { input: Reservation }) => jobs.commit(input.receipt)
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
          }),
          onError: { target: 'idle' }
        },
        after: { 4_000: { target: 'idle' } }
      },
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
