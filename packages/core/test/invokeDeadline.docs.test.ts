import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  createActor,
  createAsyncLogic,
  createMachine,
  SimulatedClock,
  TimeoutError
} from '../src/index.ts';
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
  it('reaches retryNow when the message arrives before the deadline', async () => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(createRetryMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(3_999);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('retryNow');
    expect(items).toEqual([]);
    actor.stop();
  });

  it('reaches retryLater and leaves a late message in the queue', async () => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(createRetryMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(4_000);
    expect(actor.getSnapshot().value).toBe('retryLater');
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(0);
    expect(items).toEqual(['config changed']);
    actor.stop();
  });

  it.each([
    ['the message timer was armed first', true, 'retryNow'],
    ['the deadline was armed first', false, 'retryLater']
  ] as const)(
    'reports the message it took at a tie when %s',
    async (_description, messageFirst, expectedValue) => {
      const { queue, items } = createMessageQueue();
      const offerAtDeadline = () =>
        setTimeout(() => queue.offer('config changed'), 4_000);
      if (messageFirst) offerAtDeadline();
      const actor = createActor(createRetryMachine(queue)).start();
      if (!messageFirst) offerAtDeadline();
      await vi.advanceTimersByTimeAsync(4_000);
      const { value } = actor.getSnapshot();
      // Either outcome is valid at a tie. Taking the message and then
      // reporting the deadline is not.
      expect(['retryNow', 'retryLater']).toContain(value);
      expect(items).toEqual(value === 'retryNow' ? [] : ['config changed']);
      // Both timers end up on the host's setTimeout, which vitest fakes here.
      // The actor's default clock arms the deadline, and the test arms the
      // offer. Timers due at the same time fire in the order they were armed.
      expect(value).toBe(expectedValue);
      actor.stop();
    }
  );

  it('reaches retryNow when a simulated clock advances in a later task than the offer', async () => {
    const { queue, items } = createMessageQueue();
    const clock = new SimulatedClock();
    const actor = createActor(createRetryMachine(queue), { clock }).start();
    clock.increment(3_999);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('retryNow');
    clock.increment(1);
    expect(actor.getSnapshot().value).toBe('retryNow');
    expect(items).toEqual([]);
    actor.stop();
  });

  it('reaches failed when take rejects with an error other than a timeout', async () => {
    const { queue } = createMessageQueue(new Error('queue closed'));
    const actor = createActor(createRetryMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('failed');
    actor.stop();
  });
});

// The docs say a child that follows the three rules does not lose a result
// to a machine timer on createActor with the default clock and real timers.
// The async fake-timer API yields between timers the way real timers do.
// These machines have no deadline in the logic, only a take-only,
// abort-aware child.
describe.each([
  [
    'after',
    (queue: MessageQueue) => ({
      invoke: {
        src: createAsyncLogic({ run: ({ signal }) => queue.take(signal) }),
        onDone: { target: 'retryNow' }
      },
      after: { 4_000: { target: 'retryLater' } }
    })
  ],
  [
    'an invoke timeout',
    (queue: MessageQueue) => ({
      invoke: {
        src: createAsyncLogic({ run: ({ signal }) => queue.take(signal) }),
        timeout: 4_000,
        onDone: { target: 'retryNow' },
        onTimeout: { target: 'retryLater' }
      }
    })
  ],
  [
    'a state timeout',
    (queue: MessageQueue) => ({
      invoke: {
        src: createAsyncLogic({ run: ({ signal }) => queue.take(signal) }),
        onDone: { target: 'retryNow' }
      },
      timeout: 4_000,
      onTimeout: { target: 'retryLater' }
    })
  ]
] as const)('a take-only child racing %s on createActor', (_shape, waiting) => {
  const createTimerMachine = (queue: MessageQueue) =>
    createMachine({
      initial: 'waiting',
      states: { waiting: waiting(queue), retryNow: {}, retryLater: {} }
    });

  it('reaches retryNow when the message arrives before the timer', async () => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(createTimerMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(3_999);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(1);
    expect(actor.getSnapshot().value).toBe('retryNow');
    expect(items).toEqual([]);
    actor.stop();
  });

  it('reaches retryLater and leaves a late message in the queue', async () => {
    const { queue, items } = createMessageQueue();
    const actor = createActor(createTimerMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(4_000);
    expect(actor.getSnapshot().value).toBe('retryLater');
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(0);
    expect(items).toEqual(['config changed']);
    actor.stop();
  });

  it.each([
    ['the message timer was armed first', true, 'retryNow'],
    ['the timer was armed first', false, 'retryLater']
  ] as const)(
    'does not lose the message at a tie when %s',
    async (_description, messageFirst, expectedValue) => {
      const { queue, items } = createMessageQueue();
      const offerAtDeadline = () =>
        setTimeout(() => queue.offer('config changed'), 4_000);
      if (messageFirst) offerAtDeadline();
      const actor = createActor(createTimerMachine(queue)).start();
      if (!messageFirst) offerAtDeadline();
      await vi.advanceTimersByTimeAsync(4_000);
      const { value } = actor.getSnapshot();
      expect(items).toEqual(value === 'retryNow' ? [] : ['config changed']);
      expect(value).toBe(expectedValue);
      actor.stop();
    }
  );
});

describe('ways the documented example still loses a message', () => {
  it('loses a message when take removes it and settles in a later task', async () => {
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
    const actor = createActor(createRetryMachine(queue)).start();
    await vi.advanceTimersByTimeAsync(3_960);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(100);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(server).toEqual([]);
    actor.stop();
  });

  it('loses a message when a simulated clock advances in the task that offers it', async () => {
    // The take settles but has not reported when the clock fires the deadline.
    const { queue, items } = createMessageQueue();
    const clock = new SimulatedClock();
    const actor = createActor(createRetryMachine(queue), { clock }).start();
    clock.increment(3_999);
    queue.offer('config changed');
    clock.increment(1);
    await vi.advanceTimersByTimeAsync(0);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(items).toEqual([]);
    actor.stop();
  });

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

describe('a deadline inside the logic with a child that keeps working after the take', () => {
  it('loses a message when the deadline lands inside that work', async () => {
    // The child takes the message at 3.5 seconds and saves it for a second.
    // The deadline at 4 seconds aborts the child before it returns.
    const { queue, items } = createMessageQueue();
    const machine = createMachine({
      initial: 'waiting',
      states: {
        waiting: {
          invoke: {
            src: createAsyncLogic({
              timeout: 4_000,
              run: async ({ signal }) => {
                const value = await queue.take(signal);
                await new Promise((resolvePromise) =>
                  setTimeout(resolvePromise, 1_000)
                );
                return value;
              }
            }),
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
    const actor = createActor(machine).start();
    await vi.advanceTimersByTimeAsync(3_500);
    queue.offer('config changed');
    await vi.advanceTimersByTimeAsync(1_500);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(items).toEqual([]);
    actor.stop();
  });
});

describe('ways a machine without a deadline in the logic loses a message', () => {
  it('drops a result that settles in the task that exits the invoking state', async () => {
    // The behavior the docs warn about: the completion and the exiting event
    // are two events, and the one handled first wins.
    const { queue, items } = createMessageQueue();
    const machine = createMachine({
      initial: 'waiting',
      states: {
        waiting: {
          invoke: {
            src: createAsyncLogic({ run: ({ signal }) => queue.take(signal) }),
            onDone: { target: 'retryNow' }
          },
          on: { giveUp: { target: 'retryLater' } }
        },
        retryNow: {},
        retryLater: {}
      }
    });
    const actor = createActor(machine).start();
    await vi.advanceTimersByTimeAsync(1);
    queue.offer('config changed');
    actor.send({ type: 'giveUp' });
    await vi.advanceTimersByTimeAsync(1);
    expect(actor.getSnapshot().value).toBe('retryLater');
    expect(items).toEqual([]);
    actor.stop();
  });

  describe('a child that keeps working after the take, racing a timer on the invoking state', () => {
    // The child keeps awaiting after it took the message, so the timer
    // exits the state before the child reports.
    const consumeThenSave = (queue: MessageQueue) =>
      createAsyncLogic({
        run: async ({ signal }) => {
          const value = await queue.take(signal);
          await new Promise((resolvePromise) =>
            setTimeout(resolvePromise, 1_000)
          );
          return value;
        }
      });

    it.each([
      [
        'after',
        (queue: MessageQueue) => ({
          invoke: {
            src: consumeThenSave(queue),
            onDone: { target: 'retryNow' }
          },
          after: { 4_000: { target: 'retryLater' } }
        })
      ],
      [
        'an invoke timeout',
        (queue: MessageQueue) => ({
          invoke: {
            src: consumeThenSave(queue),
            timeout: 4_000,
            onDone: { target: 'retryNow' },
            onTimeout: { target: 'retryLater' }
          }
        })
      ],
      [
        'a state timeout',
        (queue: MessageQueue) => ({
          invoke: {
            src: consumeThenSave(queue),
            onDone: { target: 'retryNow' }
          },
          timeout: 4_000,
          onTimeout: { target: 'retryLater' }
        })
      ]
    ] as const)(
      'drops a message the child took when %s exits first',
      async (_description, waiting) => {
        const { queue, items } = createMessageQueue();
        const machine = createMachine({
          initial: 'waiting',
          states: { waiting: waiting(queue), retryNow: {}, retryLater: {} }
        });
        const actor = createActor(machine).start();
        await vi.advanceTimersByTimeAsync(3_500);
        queue.offer('config changed');
        await vi.advanceTimersByTimeAsync(1_500);
        expect(actor.getSnapshot().value).toBe('retryLater');
        expect(items).toEqual([]);
        actor.stop();
      }
    );
  });
});
