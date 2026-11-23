import { createAsyncLogic, setup } from 'xstate';

type Reservation = { id: string; job: string };

/** Your broker. A reservation hides the job until it is committed or expires. */
export interface JobQueue {
  reserve(signal: AbortSignal): Promise<Reservation>;
  commit(id: string): Promise<void>;
}

export function createJobMachine(jobs: JobQueue) {
  const reserveJob = createAsyncLogic({
    run: ({ signal }): Promise<Reservation> => jobs.reserve(signal)
  });

  return setup({ actors: { reserveJob } }).createMachine({
    context: { job: null as string | null },
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'reserveJob',
          // Only this transition commits. If the timer exits first, the
          // reservation expires and the broker offers the job again.
          onDone: ({ event }, enq) => {
            enq(() => jobs.commit(event.output.id));
            return { target: 'accepted', context: { job: event.output.job } };
          }
        },
        after: { 4_000: { target: 'idle' } }
      },
      accepted: {},
      idle: {}
    }
  });
}
