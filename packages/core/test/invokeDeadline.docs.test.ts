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
const takeThenSave = (queue: MessageQueue, timeout?: number) =>
  createAsyncLogic({
    timeout,
    run: async ({ signal }) => {
      const value = await queue.take(signal);
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 1_000));
      return value;
    }
  });

const timers = [
  [
    'after',
    (src: AnyActorLogic) => ({
      invoke: { src, onDone },
      after: { 4_000: { target: 'retryLater' } }
    })
  ],
  [
    'an invoke timeout',
    (src: AnyActorLogic) => ({
      invoke: {
        src,
        timeout: 4_000,
        onDone,
        onTimeout: { target: 'retryLater' }
      }
    })
  ],
  [
    'a state timeout',
    (src: AnyActorLogic) => ({
      invoke: { src, onDone },
      timeout: 4_000,
      onTimeout: { target: 'retryLater' }
    })
  ]
] as const;

// Arms a plain timer that offers the message at the 4 second deadline,
// before or after the actor arms its own timer.
function startAtTie(
  machine: AnyStateMachine,
  queue: { offer(value: string): void },
  messageFirst: boolean
) {
  const offerAtDeadline = () =>
    setTimeout(() => queue.offer('config changed'), 4_000);
  if (messageFirst) offerAtDeadline();
  const actor = createActor(machine).start();
  if (!messageFirst) offerAtDeadline();
  return actor;
}

// Both timers end up on the global setTimeout, which vitest fakes here.
// Timers due at the same time fire in the order they were armed. Either
// winner is valid at a tie. Taking the message and then reporting the
// deadline is not.
const ties = [
  [
    'the message timer was armed first',
    true,
    { value: 'retryNow', message: 'config changed', items: [] }
  ],
  [
    'the deadline was armed first',
    false,
    { value: 'retryLater', message: null, items: ['config changed'] }
  ]
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

describe('the documented deadline example', () => {
  it.each(ties)(
    'keeps the message at a tie when %s',
    async (_description, messageFirst, expected) => {
      const { queue, items } = createMessageQueue();
      const actor = startAtTie(createRetryMachine(queue), queue, messageFirst);
      // The async fake-timer API runs each timer in its own task, as real
      // timers do.
      await vi.advanceTimersByTimeAsync(4_000);
      expect({ ...outcome(actor), items }).toEqual(expected);
      actor.stop();
    }
  );

  it('reaches failed when take rejects with an error other than a timeout', async () => {
    const { queue } = createMessageQueue(new Error('queue closed'));
    const actor = createActor(createRetryMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('failed');
    actor.stop();
  });
});

describe.each(timers)(
  'a take-only child racing %s on createActor',
  (_timer, waiting) => {
    it.each(ties)(
      'keeps the message at a tie when %s',
      async (_description, messageFirst, expected) => {
        const { queue, items } = createMessageQueue();
        const actor = startAtTie(
          createTimerMachine(waiting(takeOnly(queue))),
          queue,
          messageFirst
        );
        await vi.advanceTimersByTimeAsync(4_000);
        expect({ ...outcome(actor), items }).toEqual(expected);
        actor.stop();
      }
    );
  }
);

describe('a targetless onDone', () => {
  it.each([
    ['after', 'retryLater'],
    ['an invoke timeout', 'waiting'],
    ['a state timeout', 'retryLater']
  ] as const)(
    'with %s, keeps the message and is in %s after the timer is due',
    async (timer, value) => {
      const { queue } = createMessageQueue();
      const waiting = timers.find(([name]) => name === timer)![1](
        takeOnly(queue)
      );
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

describe('work after the take', () => {
  // The child takes the message at 3.5 seconds and saves it for a second.
  // The timer or the deadline at 4 seconds lands inside that work.
  it.each([
    ...timers.map(
      ([timer, waiting]) =>
        [timer, (queue: MessageQueue) => waiting(takeThenSave(queue))] as const
    ),
    [
      'a deadline inside the logic',
      (queue: MessageQueue) => ({
        invoke: {
          src: takeThenSave(queue, 4_000),
          onDone,
          onError: { target: 'retryLater' }
        }
      })
    ] as const
  ])('loses a message to %s', async (_timer, waiting) => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(createTimerMachine(waiting(queue))).start();
    await vi.advanceTimersByTimeAsync(3_500);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(1_500);
    expect({ ...outcome(actor), items }).toEqual({
      value: 'retryLater',
      message: null,
      items: []
    });
    actor.stop();
  });
});

it('loses a message to the deadline inside the logic when the take settles in a later task', async () => {
  // Like a queue server that removes the message at once and answers after
  // the deadline.
  const server = ['config changed'];
  const actor = createActor(
    createRetryMachine({
      take: () => {
        const message = server.shift()!;
        return new Promise((resolvePromise) =>
          setTimeout(() => resolvePromise(message), 4_050)
        );
      }
    })
  ).start();
  await vi.advanceTimersByTimeAsync(4_100);
  expect({ ...outcome(actor), server }).toEqual({
    value: 'retryLater',
    message: null,
    server: []
  });
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
    ['in the task that offers it', true, 'retryLater'],
    ['in a later task than the offer', false, 'retryNow']
  ] as const)(
    'with a simulated clock advanced %s, reaches %s and empties the queue',
    async (_description, sameTask, expectedValue) => {
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
  function createBroker() {
    const committed: string[] = [];
    let deliver = (_job: string) => {};
    const jobs: JobQueue = {
      reserve: (signal) =>
        new Promise((resolvePromise, reject) => {
          deliver = (job) => resolvePromise({ id: `r-${job}`, job });
          signal.addEventListener('abort', () => reject(signal.reason));
        }),
      commit: async (id) => void committed.push(id)
    };
    return { jobs, committed, deliver: (job: string) => deliver(job) };
  }

  it('commits the reservation and keeps the job when the child wins', async () => {
    const broker = createBroker();
    const actor = createActor(createJobMachine(broker.jobs)).start();
    await vi.advanceTimersByTimeAsync(1_000);
    broker.deliver('build');
    await vi.advanceTimersByTimeAsync(4_000);
    const { value, context } = actor.getSnapshot();
    expect({ value, job: context.job }).toEqual({
      value: 'accepted',
      job: 'build'
    });
    expect(broker.committed).toEqual(['r-build']);
    actor.stop();
  });

  it.each([
    ['before the job arrives', false],
    ['in the task that delivers the reservation', true]
  ] as const)(
    'commits nothing when the timer exits %s',
    async (_description, sameTask) => {
      const broker = createBroker();
      if (sameTask) setTimeout(() => broker.deliver('build'), 4_000);
      const actor = createActor(createJobMachine(broker.jobs)).start();
      // Synchronous fake timers fire the delivery and the timer in one task.
      vi.advanceTimersByTime(4_000);
      await vi.advanceTimersByTimeAsync(0);
      expect(actor.getSnapshot().value).toBe('idle');
      expect(broker.committed).toEqual([]);
      actor.stop();
    }
  );
});
