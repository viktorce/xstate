import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createActor, createAsyncLogic, createMachine } from '../src/index.ts';
import {
  createRetryMachine,
  type SignalQueue
} from './fixtures/invoke-deadline/take-or-deadline.ts';

/** A queue whose pending take is withdrawn when its signal aborts. */
function createSignalQueue() {
  const items: string[] = [];
  let waiters: Array<(value: string) => void> = [];
  const queue: SignalQueue & { offer(value: string): void } = {
    offer(value) {
      const waiter = waiters.shift();
      if (waiter) waiter(value);
      else items.push(value);
    },
    take(signal) {
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

it('takes onDone when the signal arrives before the deadline', async () => {
  const { queue, items } = createSignalQueue();
  const actor = createActor(createRetryMachine(queue)).start();
  await vi.advanceTimersByTimeAsync(3_999);
  queue.offer('config changed');
  await vi.advanceTimersByTimeAsync(0);
  expect(actor.getSnapshot().value).toBe('retryNow');
  expect(items).toEqual([]);
  actor.stop();
});

it('leaves a late signal in the queue when the deadline wins', async () => {
  const { queue, items } = createSignalQueue();
  const actor = createActor(createRetryMachine(queue)).start();
  await vi.advanceTimersByTimeAsync(4_000);
  expect(actor.getSnapshot().value).toBe('retryLater');
  queue.offer('config changed');
  await vi.advanceTimersByTimeAsync(0);
  expect(items).toEqual(['config changed']);
  actor.stop();
});

it.each([
  ['the signal timer was armed first', true],
  ['the deadline was armed first', false]
] as const)(
  'never consumes a signal it does not report when %s',
  async (_description, signalFirst) => {
    const { queue, items } = createSignalQueue();
    const offerAtDeadline = () =>
      setTimeout(() => queue.offer('config changed'), 4_000);
    if (signalFirst) offerAtDeadline();
    const actor = createActor(createRetryMachine(queue)).start();
    if (!signalFirst) offerAtDeadline();
    await vi.advanceTimersByTimeAsync(4_000);
    const { value } = actor.getSnapshot();
    // Either outcome is valid at a tie. Taking the signal and then
    // reporting the deadline is not.
    expect(['retryNow', 'retryLater']).toContain(value);
    expect(items).toEqual(value === 'retryNow' ? [] : ['config changed']);
    actor.stop();
  }
);

it('drops a result that settles in the task that exits the invoking state', async () => {
  // The behavior the docs warn about: the completion and the exiting event
  // are two events, and the one handled first wins.
  const { queue, items } = createSignalQueue();
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
