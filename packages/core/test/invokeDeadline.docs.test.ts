import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
// Imported from 'xstate' like the fixtures, so both resolve to the same
// module and types whether the package is linked to source or built.
import {
  createActor,
  createAsyncLogic,
  createMachine,
  SimulatedClock,
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

/** A queue whose pending take is withdrawn when its abort signal aborts. */
function createMessageQueue(failure?: Error) {
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
      if (items.length) return Promise.resolve(items.shift()!);
      return new Promise((resolvePromise, reject) => {
        const waiter = (value: string) => resolvePromise(value);
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

// The case the docs call work after the take.
const takeThenSave = (queue: MessageQueue) =>
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
// after the actor arms its own timer.
function startAtTie(
  machine: AnyStateMachine,
  queue: { offer(value: string): void },
  messageFirst: boolean,
  ms = 4_000
) {
  const offerAtDeadline = () =>
    setTimeout(() => queue.offer('config changed'), ms);
  if (messageFirst) offerAtDeadline();
  const actor = createActor(machine).start();
  if (!messageFirst) offerAtDeadline();
  return actor;
}

// Both timers end up on the global setTimeout, which vitest fakes here.
// Timers due at the same time fire in the order they were armed. Either
// winner is valid at a tie. Taking the message and then reporting the
// deadline is not.
const won = { value: 'retryNow', message: 'config changed', items: [] };
const kept = { value: 'retryLater', message: null, items: ['config changed'] };
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
      const actor = startAtTie(createRacer(queue), queue, messageFirst);
      // The async fake-timer API runs each timer in its own task, as real
      // timers do.
      await vi.advanceTimersByTimeAsync(4_000);
      expect({ ...outcome(actor), items }).toEqual(expected);
      actor.stop();
    }
  );
});

it.each(timerNames)(
  'keeps the message at a tie with %s and real timers',
  async (timer) => {
    vi.useRealTimers();
    const runs = [true, false].map(async (messageFirst) => {
      const { queue, items } = createMessageQueue();
      const machine = createTimerMachine(timers[timer](takeOnly(queue), 20));
      const actor = startAtTie(machine, queue, messageFirst, 20);
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 40));
      actor.stop();
      return { ...outcome(actor), items };
    });
    // Real timers do not promise an order, so either winner is valid.
    for (const result of await Promise.all(runs)) {
      expect([won, kept]).toContainEqual(result);
    }
  }
);

it('reaches failed when take rejects with an error other than a timeout', async () => {
  const { queue } = createMessageQueue(new Error('queue closed'));
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

it('loses a message to an after timer that fires during work after the take', async () => {
  // The child takes the message at 3.5 seconds and saves it for a second.
  const { queue, items } = createMessageQueue();
  const actor = createActor(
    createTimerMachine(timers.after(takeThenSave(queue)))
  ).start();
  await vi.advanceTimersByTimeAsync(3_500);
  queue.offer('config changed');
  await vi.advanceTimersByTimeAsync(1_500);
  expect({ ...outcome(actor), items }).toEqual({ ...kept, items: [] });
  actor.stop();
});

describe('an exit in the same task', () => {
  it.each([
    [
      'loses the message to an event sent in the task that settles the take',
      true,
      { value: 'retryLater', message: null, items: [] }
    ],
    [
      'keeps the message when the event is sent in a later task',
      false,
      { value: 'retryNow', message: 'config changed', items: [] }
    ]
  ] as const)('%s', async (_description, sameTask, expected) => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(
      createTimerMachine({
        invoke: { src: takeOnly(queue), onDone },
        on: { giveUp: { target: 'retryLater' } }
      })
    ).start();
    await vi.advanceTimersByTimeAsync(1);
    queue.offer('config changed');
    if (!sameTask) await vi.advanceTimersByTimeAsync(1);
    // In retryNow the event has no transition, which logs a warning.
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    actor.send({ type: 'giveUp' });
    await vi.advanceTimersByTimeAsync(1);
    expect({ ...outcome(actor), items }).toEqual(expected);
    actor.stop();
  });

  it.each([
    ['in the task that offers it', 'retryLater', true],
    ['in a later task than the offer', 'retryNow', false]
  ] as const)(
    'with a simulated clock advanced %s, reaches %s and empties the queue',
    async (_description, expectedValue, sameTask) => {
      const { queue, items } = createMessageQueue();
      const clock = new SimulatedClock();
      const actor = createActor(createRetryMachine(queue), { clock }).start();
      clock.increment(3_999);
      queue.offer('config changed');
      if (sameTask) clock.increment(1);
      await vi.advanceTimersByTimeAsync(0);
      clock.increment(1);
      expect(actor.getSnapshot().value).toBe(expectedValue);
      expect(items).toEqual([]);
      actor.stop();
    }
  );

  it('loses a message when synchronous fake timers fire the offer and the deadline in one task', async () => {
    // The offer is armed first, so it fires first, but the take has not
    // reported when the deadline fires in the same task.
    const { queue, items } = createMessageQueue();
    setTimeout(() => queue.offer('config changed'), 4_000);
    const actor = createActor(createRetryMachine(queue)).start();
    vi.advanceTimersByTime(4_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(items).toEqual([]);
    actor.stop();
  });
});

describe('the documented reserve, then commit example', () => {
  // Hides a reserved job for 10 seconds and offers it again unless the
  // reservation is committed by then.
  function createBroker(failCommit = false) {
    const ready: string[] = [];
    const log: string[] = [];
    const expiries = new Map<string, ReturnType<typeof setTimeout>>();
    let waiter: ((job: string) => void) | undefined;
    const offer = (job: string) => {
      const take = waiter;
      waiter = undefined;
      return take ? take(job) : ready.push(job);
    };
    const reserve = (job: string) => {
      expiries.set(
        job,
        setTimeout(() => offer(job), 10_000)
      );
      return { id: job, job };
    };
    const jobs: JobQueue = {
      reserve: (signal) =>
        new Promise((resolvePromise, reject) => {
          waiter = (job) => resolvePromise(reserve(job));
          signal.addEventListener('abort', () => {
            waiter = undefined;
            reject(signal.reason);
          });
        }),
      commit: async (id) => {
        if (failCommit) throw new Error('broker unavailable');
        clearTimeout(expiries.get(id));
        log.push(`committed ${id}`);
      }
    };
    const work = async (job: string) => {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 1_000));
      log.push(`worked ${job}`);
    };
    return { machine: createJobMachine(jobs, work), offer, ready, log };
  }

  it.each([
    [
      'commits after the work succeeds',
      false,
      { value: 'done', log: ['worked build', 'committed build'], ready: [] }
    ],
    [
      'stays in idle when the commit fails, and the job returns',
      true,
      { value: 'idle', log: ['worked build'], ready: ['build'] }
    ]
  ] as const)('%s', async (_description, failCommit, expected) => {
    const broker = createBroker(failCommit);
    const actor = createActor(broker.machine).start();
    broker.offer('build');
    await vi.advanceTimersByTimeAsync(11_000);
    const { value, status } = actor.getSnapshot();
    const { log, ready } = broker;
    expect({ value, status, log, ready }).toEqual({
      status: 'active',
      ...expected
    });
    actor.stop();
  });

  it('commits nothing when the timer exits in the task that delivers the reservation, and the job returns', async () => {
    const broker = createBroker();
    setTimeout(() => broker.offer('build'), 4_000);
    const actor = createActor(broker.machine).start();
    // Synchronous fake timers fire the delivery and the timer in one task.
    vi.advanceTimersByTime(4_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('idle');
    expect(broker.ready).toEqual([]);
    await vi.advanceTimersByTimeAsync(10_000);
    expect({ log: broker.log, ready: broker.ready }).toEqual({
      log: [],
      ready: ['build']
    });
    actor.stop();
  });
});
