import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
// Imported from 'xstate' like the fixture, so both resolve to the same
// module and types whether the package is linked to source or built.
import {
  createActor,
  createAsyncLogic,
  createMachine,
  SimulatedClock,
  TimeoutError,
  type AnyActorLogic,
  type AnyStateMachine,
  type InspectionEvent
} from 'xstate';
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

/** Starts the machine and records the output of each child completion. */
function start(machine: AnyStateMachine, clock?: SimulatedClock) {
  const reported: unknown[] = [];
  const inspect = (inspectionEvent: InspectionEvent) => {
    if (
      inspectionEvent.type === '@xstate.transition' &&
      inspectionEvent.event.type === 'xstate.done.actor'
    ) {
      reported.push((inspectionEvent.event as { output?: unknown }).output);
    }
  };
  const options = clock ? { clock, inspect } : { inspect };
  return { actor: createActor(machine, options).start(), reported };
}

const createTimerMachine = (waiting: object) =>
  createMachine({
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
      invoke: { src, onDone: { target: 'retryNow' } },
      after: { 4_000: { target: 'retryLater' } }
    })
  ],
  [
    'an invoke timeout',
    (src: AnyActorLogic) => ({
      invoke: {
        src,
        timeout: 4_000,
        onDone: { target: 'retryNow' },
        onTimeout: { target: 'retryLater' }
      }
    })
  ],
  [
    'a state timeout',
    (src: AnyActorLogic) => ({
      invoke: { src, onDone: { target: 'retryNow' } },
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
  const started = start(machine);
  if (!messageFirst) offerAtDeadline();
  return started;
}

// Both timers end up on the global setTimeout, which vitest fakes here.
// Timers due at the same time fire in the order they were armed, so each row
// pins the winner and both branches of the item check run.
const ties = [
  ['the message timer was armed first', true, 'retryNow'],
  ['the deadline was armed first', false, 'retryLater']
] as const;

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
});

it('keeps the documented example in sync with its fixture', () => {
  const docs = readFileSync(
    resolve(__dirname, '../../../docs/invoke.md'),
    'utf8'
  );
  const source = readFileSync(
    resolve(__dirname, 'fixtures/invoke-deadline/take-or-deadline.ts'),
    'utf8'
  );
  expect(docs).toContain('```ts\n' + source.trimEnd() + '\n```');
});

describe('the documented example', () => {
  it.each(ties)(
    'reports the message it took, or leaves it in the queue, at a tie when %s',
    async (_description, messageFirst, winner) => {
      const { queue, items } = createMessageQueue();
      const { actor, reported } = startAtTie(
        createRetryMachine(queue),
        queue,
        messageFirst
      );
      // The async fake-timer API runs each timer in its own task, as real
      // timers do.
      await vi.advanceTimersByTimeAsync(4_000);
      const { value } = actor.getSnapshot();
      expect(value).toBe(winner);
      // Either outcome is valid at a tie. Taking the message and then
      // reporting the deadline is not.
      expect({ items, reported }).toEqual(
        value === 'retryNow'
          ? { items: [], reported: ['config changed'] }
          : { items: ['config changed'], reported: [] }
      );
      actor.stop();
    }
  );

  it('reaches failed when take rejects with an error other than a timeout', async () => {
    const { queue } = createMessageQueue(new Error('queue closed'));
    const { actor } = start(createRetryMachine(queue));
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('failed');
    actor.stop();
  });
});

describe.each(timers)(
  'a take-only child racing %s on createActor',
  (_timer, waiting) => {
    it.each(ties)(
      'does not lose the message at a tie when %s',
      async (_description, messageFirst, winner) => {
        const { queue, items } = createMessageQueue();
        const { actor, reported } = startAtTie(
          createTimerMachine(waiting(takeOnly(queue))),
          queue,
          messageFirst
        );
        await vi.advanceTimersByTimeAsync(4_000);
        const { value } = actor.getSnapshot();
        expect(value).toBe(winner);
        expect({ items, reported }).toEqual(
          value === 'retryNow'
            ? { items: [], reported: ['config changed'] }
            : { items: ['config changed'], reported: [] }
        );
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
    'with %s, is in %s after the timer is due',
    async (timer, expectedValue) => {
      const { queue } = createMessageQueue();
      const waiting = timers.find(([name]) => name === timer)![1](
        takeOnly(queue)
      );
      const { actor, reported } = start(
        createTimerMachine({
          ...waiting,
          invoke: { ...waiting.invoke, onDone: () => ({}) }
        })
      );
      queue.offer('config changed');
      await vi.advanceTimersByTimeAsync(0);
      expect(reported).toEqual(['config changed']);
      await vi.advanceTimersByTimeAsync(4_000);
      expect(actor.getSnapshot().value).toBe(expectedValue);
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
          onDone: { target: 'retryNow' },
          onError: { target: 'retryLater' }
        }
      })
    ] as const
  ])('loses a message to %s', async (_timer, waiting) => {
    const { queue, items } = createMessageQueue();
    const { actor, reported } = start(createTimerMachine(waiting(queue)));
    await vi.advanceTimersByTimeAsync(3_500);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(1_500);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect({ items, reported }).toEqual({ items: [], reported: [] });
    actor.stop();
  });
});

describe('a take that settles later', () => {
  it('loses a message to the deadline inside the documented example', async () => {
    // Like a network request: the server removes the item at once and the
    // response arrives 50 ms later. The deadline aborts in between.
    const server: string[] = [];
    let waiters: Array<() => void> = [];
    const queue: MessageQueue & { offer(value: string): void } = {
      offer(value) {
        server.push(value);
        waiters.shift()?.();
      },
      take(signal) {
        return new Promise((resolvePromise, reject) => {
          let response: ReturnType<typeof setTimeout> | undefined;
          const remove = () => {
            const value = server.shift()!;
            response = setTimeout(() => resolvePromise(value), 50);
          };
          signal.addEventListener('abort', () => {
            waiters = waiters.filter((candidate) => candidate !== remove);
            clearTimeout(response);
            reject(signal.reason);
          });
          if (server.length) remove();
          else waiters.push(remove);
        });
      }
    };
    const { actor } = start(createRetryMachine(queue));
    await vi.advanceTimersByTimeAsync(3_960);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(100);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(server).toEqual([]);
    actor.stop();
  });
});

describe('an exit in the same task', () => {
  it('loses a message to an event sent in the task that settles the take', async () => {
    const { queue, items } = createMessageQueue();
    const { actor } = start(
      createMachine({
        initial: 'waiting',
        states: {
          waiting: {
            invoke: { src: takeOnly(queue), onDone: { target: 'retryNow' } },
            on: { giveUp: { target: 'retryLater' } }
          },
          retryNow: {},
          retryLater: {}
        }
      })
    );
    await vi.advanceTimersByTimeAsync(1);
    queue.offer('config changed');
    actor.send({ type: 'giveUp' });
    await vi.advanceTimersByTimeAsync(1);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(items).toEqual([]);
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
      const { actor } = start(createRetryMachine(queue), clock);
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
    const { actor } = start(createRetryMachine(queue));
    vi.advanceTimersByTime(4_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(items).toEqual([]);
    actor.stop();
  });
});
