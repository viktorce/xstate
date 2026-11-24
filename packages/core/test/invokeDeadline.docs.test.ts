import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
// Imported from 'xstate' like the fixtures, so both resolve to the same
// module and types whether the package is linked to source or built.
import {
  createActor,
  createAsyncLogic,
  createMachine,
  SimulatedClock,
  waitFor,
  type AnyActorLogic,
  type AnyStateMachine
} from 'xstate';
import {
  createJobMachine,
  type JobQueue
} from './fixtures/invoke-deadline/reserve-then-commit.ts';
import {
  createRetryMachine,
  type MessageQueue
} from './fixtures/invoke-deadline/take-or-deadline.ts';

/**
 * A queue that follows rule 1. With `settleAfter`, a take removes the offered
 * item at once and settles that many milliseconds later.
 */
function createMessageQueue({
  failure,
  settleAfter
}: { failure?: Error; settleAfter?: number } = {}) {
  const items: string[] = [];
  let waiters: Array<(value: string) => void> = [];
  const queue: MessageQueue & { offer(value: string): void } = {
    offer(value) {
      const waiter = waiters.shift();
      if (waiter) waiter(value);
      else items.push(value);
    },
    take(signal) {
      if (failure) return Promise.reject(failure);
      if (signal.aborted) return Promise.reject(signal.reason);
      if (items.length) return Promise.resolve(items.shift()!);
      return new Promise((resolvePromise, reject) => {
        const waiter = (value: string) =>
          settleAfter === undefined
            ? resolvePromise(value)
            : setTimeout(() => resolvePromise(value), settleAfter);
        signal.addEventListener('abort', () => {
          waiters = waiters.filter((candidate) => candidate !== waiter);
          reject(signal.reason);
        });
        waiters.push(waiter);
      });
    }
  };
  return { queue, items };
}

const outcome = (actor: {
  getSnapshot(): { value: unknown; context: any };
}) => {
  const { value, context } = actor.getSnapshot();
  return { value, message: context.message };
};

// Stores the message like the documented example, so a reported message is
// visible in context.
const onDone = ({ event }: { event: { output: unknown } }) => ({
  target: 'retryNow',
  context: { message: event.output }
});

const createTimerMachine = (waiting: object) =>
  createMachine({
    context: { message: null as unknown },
    initial: 'waiting',
    states: { waiting, retryNow: {}, retryLater: {} }
  });

const takeOnly = (queue: MessageQueue) =>
  createAsyncLogic({ run: ({ signal }) => queue.take(signal) });

// Passes `signal` to the take and returns a second after it settles. This is
// the case the docs call asynchronous work after the take.
const takeThenWait = (queue: MessageQueue) =>
  createAsyncLogic({
    run: async ({ signal }) => {
      const value = await queue.take(signal);
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 1_000));
      return value;
    }
  });

const timers = {
  after: (src: AnyActorLogic, ms = 4_000) => ({
    invoke: { src, onDone },
    after: { [ms]: { target: 'retryLater' } }
  }),
  'an invoke timeout': (src: AnyActorLogic, ms = 4_000) => ({
    invoke: { src, timeout: ms, onDone, onTimeout: { target: 'retryLater' } }
  }),
  'a state timeout': (src: AnyActorLogic, ms = 4_000) => ({
    invoke: { src, onDone },
    timeout: ms,
    onTimeout: { target: 'retryLater' }
  })
};
type Timer = keyof typeof timers;
const timerNames = Object.keys(timers) as Timer[];

// Arms a plain timer that offers the message at the deadline, before or
// after the actor arms its own timer. `offered` settles after the offer.
function startAtTie(
  machine: AnyStateMachine,
  queue: { offer(value: string): void },
  messageFirst: boolean,
  ms = 4_000
) {
  let offered!: Promise<void>;
  const offerAtDeadline = () => {
    offered = new Promise((resolvePromise) =>
      setTimeout(() => resolvePromise(queue.offer('config changed')), ms)
    );
  };
  if (messageFirst) offerAtDeadline();
  const actor = createActor(machine).start();
  if (!messageFirst) offerAtDeadline();
  return { actor, offered };
}

// Both timers end up on the global setTimeout, which vitest fakes here.
// Timers due at the same time fire in the order they were armed. Either
// winner is valid at a tie. Taking the message and then reporting the
// deadline is not.
const won = { value: 'retryNow', message: 'config changed', items: [] };
const kept = { value: 'retryLater', message: null, items: ['config changed'] };
const lost = { ...kept, items: [] };
const ties = [
  ['the message timer was armed first', true, won],
  ['the deadline was armed first', false, kept]
] as const;

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

// The @xstate/effect race test imports take-or-deadline.ts as well.
it.each(['take-or-deadline.ts', 'reserve-then-commit.ts'])(
  'keeps the documented %s in sync with its fixture',
  (fixture) => {
    const docs = readFileSync(
      resolve(__dirname, '../../../docs/invoke.md'),
      'utf8'
    );
    const source = readFileSync(
      resolve(__dirname, 'fixtures/invoke-deadline', fixture),
      'utf8'
    );
    expect(docs).toContain('```ts\n' + source.trimEnd() + '\n```');
  }
);

describe.each([
  ...timerNames.map(
    (timer) =>
      [
        `a take-only child racing ${timer}`,
        (queue: MessageQueue) =>
          createTimerMachine(timers[timer](takeOnly(queue)))
      ] as const
  ),
  ['the documented deadline example', createRetryMachine] as const
])('%s on createActor', (_racer, createRacer) => {
  it.each(ties)(
    'keeps the message at a tie when %s',
    async (_description, messageFirst, expected) => {
      const { queue, items } = createMessageQueue();
      const { actor } = startAtTie(createRacer(queue), queue, messageFirst);
      // Async fake timers run each timer in its own turn, as real timers do.
      await vi.advanceTimersByTimeAsync(4_000);
      expect({ ...outcome(actor), items }).toEqual(expected);
      actor.stop();
    }
  );
});

it.each(timerNames)(
  'keeps the message with %s and equal delays on real timers',
  async (timer) => {
    vi.useRealTimers();
    // Five rounds at equal delays in both arming orders. Real timers do not
    // promise an order, so either winner is valid.
    for (let round = 0; round < 5; round++) {
      const runs = [true, false].map(async (messageFirst) => {
        const { queue, items } = createMessageQueue();
        const machine = createTimerMachine(timers[timer](takeOnly(queue), 5));
        const { actor, offered } = startAtTie(machine, queue, messageFirst, 5);
        await Promise.all([
          offered,
          waitFor(actor, (s) => s.value !== 'waiting', { timeout: 1_000 })
        ]);
        actor.stop();
        return { ...outcome(actor), items };
      });
      for (const result of await Promise.all(runs)) {
        expect([won, kept]).toContainEqual(result);
      }
    }
  }
);

it('reaches failed when take rejects with an error other than a timeout', async () => {
  const { queue } = createMessageQueue({ failure: new Error('queue closed') });
  const actor = createActor(createRetryMachine(queue)).start();
  await vi.advanceTimersByTimeAsync(0);
  expect(actor.getSnapshot().value).toBe('failed');
  actor.stop();
});

describe('a targetless onDone', () => {
  it.each([
    ['after', 'retryLater'],
    ['an invoke timeout', 'waiting'],
    ['a state timeout', 'retryLater']
  ] as const)(
    'with %s, keeps the message and is in %s after the timer is due',
    async (timer, value) => {
      const { queue } = createMessageQueue();
      const waiting = timers[timer](takeOnly(queue));
      const actor = createActor(
        createTimerMachine({
          ...waiting,
          invoke: {
            ...waiting.invoke,
            onDone: ({ event }: { event: { output: unknown } }) => ({
              context: { message: event.output }
            })
          }
        })
      ).start();
      queue.offer('config changed');
      await vi.advanceTimersByTimeAsync(4_000);
      expect(outcome(actor)).toEqual({ value, message: 'config changed' });
      actor.stop();
    }
  );
});

// Rejects when signal aborts but leaves its request with the queue.
const takeWithoutWithdrawing = (queue: MessageQueue) =>
  createAsyncLogic({
    run: ({ signal }) =>
      new Promise<string>((resolvePromise, reject) => {
        void queue.take(new AbortController().signal).then(resolvePromise);
        signal.addEventListener('abort', () => reject(signal.reason));
      })
  });

// An after timer fires at 4 seconds in each case.
it.each<[string, (queue: MessageQueue) => AnyActorLogic, number, number?]>([
  [
    'a take that rejects without withdrawing its request, offered long after the timer',
    takeWithoutWithdrawing,
    10_000
  ],
  ['work after the take, offered at 3.5 seconds', takeThenWait, 3_500],
  [
    'a take that settles 100 ms after it removes the message, offered at 3.95 seconds',
    takeOnly,
    3_950,
    100
  ]
])(
  'loses the message to %s',
  async (_case, createChild, offerAt, settleAfter) => {
    const { queue, items } = createMessageQueue({ settleAfter });
    const actor = createActor(
      createTimerMachine(timers.after(createChild(queue)))
    ).start();
    await vi.advanceTimersByTimeAsync(offerAt);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(2_000);
    expect({ ...outcome(actor), items }).toEqual(lost);
    actor.stop();
  }
);

it('keeps the message when the take starts after the timer aborted signal', async () => {
  const { queue, items } = createMessageQueue();
  const abortedAtTake: boolean[] = [];
  const take = queue.take;
  vi.spyOn(queue, 'take').mockImplementation((signal) => {
    abortedAtTake.push(signal.aborted);
    return take(signal);
  });
  const awaitSetupThenTake = createAsyncLogic({
    run: async ({ signal }) => {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 5_000));
      return queue.take(signal);
    }
  });
  const actor = createActor(
    createTimerMachine(timers.after(awaitSetupThenTake))
  ).start();
  await vi.advanceTimersByTimeAsync(6_000);
  queue.offer('config changed');
  expect(abortedAtTake).toEqual([true]);
  expect({ ...outcome(actor), items }).toEqual(kept);
  actor.stop();
});

describe('an exit in the same turn', () => {
  it.each([
    [
      'loses the message to an event sent in the turn that settles the take',
      true,
      lost
    ],
    ['keeps the message when the event is sent in a later turn', false, won]
  ] as const)('%s', async (_description, sameTurn, expected) => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(
      createTimerMachine({
        invoke: { src: takeOnly(queue), onDone },
        on: { giveUp: { target: 'retryLater' } }
      })
    ).start();
    await vi.advanceTimersByTimeAsync(1);
    queue.offer('config changed');
    if (!sameTurn) await vi.advanceTimersByTimeAsync(1);
    // In retryNow the event has no transition, which logs a warning.
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    actor.send({ type: 'giveUp' });
    await vi.advanceTimersByTimeAsync(1);
    expect({ ...outcome(actor), items }).toEqual(expected);
    actor.stop();
  });

  it('loses the message when an exit action of the state offers it and the timer exits', async () => {
    const { queue, items } = createMessageQueue();
    const offer = vi.fn(() => queue.offer('config changed'));
    const actor = createActor(
      createTimerMachine({
        ...timers.after(takeOnly(queue)),
        exit: (_: unknown, enq: (effect: () => void) => void) => enq(offer)
      })
    ).start();
    await vi.advanceTimersByTimeAsync(4_000);
    expect(offer).toHaveBeenCalledOnce();
    expect({ ...outcome(actor), items }).toEqual(lost);
    actor.stop();
  });

  it.each([
    ['in the turn that offers it, loses the message', true, lost],
    ['in a later turn than the offer, reports the message', false, won]
  ] as const)(
    'with a simulated clock advanced %s',
    async (_description, sameTurn, expected) => {
      const { queue, items } = createMessageQueue();
      const clock = new SimulatedClock();
      const machine = createTimerMachine(timers.after(takeOnly(queue)));
      const actor = createActor(machine, { clock }).start();
      clock.increment(3_999);
      queue.offer('config changed');
      if (sameTurn) clock.increment(1);
      await vi.advanceTimersByTimeAsync(0);
      clock.increment(1);
      expect({ ...outcome(actor), items }).toEqual(expected);
      actor.stop();
    }
  );

  it('loses a message when synchronous fake timers fire the offer and the deadline in one turn', async () => {
    // The offer is armed first, so it fires first, but the take has not
    // reported when the deadline fires in the same turn.
    const { queue, items } = createMessageQueue();
    const offer = vi.fn(() => queue.offer('config changed'));
    setTimeout(offer, 4_000);
    const actor = createActor(createRetryMachine(queue)).start();
    vi.advanceTimersByTime(4_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(offer).toHaveBeenCalledOnce();
    expect({ ...outcome(actor), items }).toEqual(lost);
    actor.stop();
  });
});

describe('the documented reserve, then commit example', () => {
  type Job = { id: string; payload: string };
  const job: Job = { id: 'job-1', payload: 'build' };

  // Hides a reserved job for 10 seconds and offers it again unless the
  // reservation is committed by then. `fail` names the step that fails. A
  // failed reserve has already hidden the job, as when the reply is lost.
  function createBroker(fail: 'nothing' | 'reserve' | 'work' | 'commit') {
    const ready: Job[] = [];
    const log: string[] = [];
    const expiries = new Map<string, ReturnType<typeof setTimeout>>();
    let receipts = 0;
    let waiter: ((next: Job) => void) | undefined;
    const offer = (next: Job) => {
      const take = waiter;
      waiter = undefined;
      return take ? take(next) : ready.push(next);
    };
    const reserve = (next: Job) => {
      const receipt = `receipt-${++receipts}`;
      const expire = () => expiries.delete(receipt) && offer(next);
      expiries.set(receipt, setTimeout(expire, 10_000));
      return fail === 'reserve'
        ? Promise.reject(new Error('reply lost'))
        : Promise.resolve({ receipt, job: next });
    };
    const jobs: JobQueue = {
      reserve: (signal) =>
        signal.aborted
          ? Promise.reject(signal.reason)
          : new Promise((resolvePromise, reject) => {
              waiter = (next) => resolvePromise(reserve(next));
              signal.addEventListener('abort', () => {
                waiter = undefined;
                reject(signal.reason);
              });
            }),
      commit: async (receipt) => {
        if (fail === 'commit') throw new Error('broker unavailable');
        if (!expiries.has(receipt)) throw new Error('reservation expired');
        clearTimeout(expiries.get(receipt));
        expiries.delete(receipt);
        log.push(`committed ${receipt}`);
      }
    };
    const work = async ({ id }: Job) => {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 1_000));
      if (fail === 'work') throw new Error('build failed');
      log.push(`worked ${id}`);
    };
    return { machine: createJobMachine(jobs, work), offer, ready, log };
  }

  // By 11 seconds a reservation that was not committed has expired.
  it.each([
    [
      'nothing',
      'done',
      'commits the job, which does not return',
      ['worked job-1', 'committed receipt-1'],
      []
    ],
    ['reserve', 'idle', 'the job returns', [], [job]],
    ['work', 'idle', 'the job returns', [], [job]],
    ['commit', 'idle', 'the job returns', ['worked job-1'], [job]]
  ] as const)(
    'when %s fails, stays active in %s and %s',
    async (fail, value, _outcome, log, ready) => {
      const broker = createBroker(fail);
      const actor = createActor(broker.machine).start();
      broker.offer(job);
      await vi.advanceTimersByTimeAsync(11_000);
      const snapshot = actor.getSnapshot();
      expect({
        value: snapshot.value,
        status: snapshot.status,
        log: broker.log,
        ready: broker.ready
      }).toEqual({ value, status: 'active', log, ready });
      actor.stop();
    }
  );

  it('commits nothing when the timer exits in the turn that delivers the reservation, and the job returns', async () => {
    const broker = createBroker('nothing');
    setTimeout(() => broker.offer(job), 4_000);
    const actor = createActor(broker.machine).start();
    // Synchronous fake timers fire the delivery and the timer in one turn.
    vi.advanceTimersByTime(4_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('idle');
    expect(broker.ready).toEqual([]);
    await vi.advanceTimersByTimeAsync(10_000);
    expect({ log: broker.log, ready: broker.ready }).toEqual({
      log: [],
      ready: [job]
    });
    actor.stop();
  });
});
