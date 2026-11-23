import { Clock, Context, Duration, Effect, Option, Queue, Scope } from 'effect';
import { TestClock } from 'effect/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createAsyncLogic, createMachine, TimeoutError, types } from 'xstate';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor
} from './index.ts';

class Signals extends Context.Service<Signals, Queue.Queue<string>>()(
  '@test/Signals'
) {}

type Outcome = { kind: 'signal'; signal: string } | { kind: 'deadline' };

const toSignal = (signal: string) => ({ kind: 'signal' as const, signal });

// The machine from examples/effect-workflows/src/deadline.ts, with a `cancel`
// event and, for the pattern the docs warn about, an `after` timer.
function createRetryMachine(
  task: Effect.Effect<Outcome, never, Signals>,
  timer?: 'after'
) {
  const nextSignal = createEffectLogic(task);
  return setupEffect({ actors: { nextSignal } }).createMachine({
    schemas: { events: { cancel: types<{}>() } },
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'nextSignal',
          onDone: ({ event }) => ({
            target: event.output.kind === 'signal' ? 'retryNow' : 'retryLater'
          })
        },
        after: timer ? { 4_000: { target: 'retryLater' } } : {},
        on: { cancel: { target: 'cancelled' } }
      },
      retryNow: { type: 'final' },
      retryLater: { type: 'final' },
      cancelled: { type: 'final' }
    }
  });
}

const takeSignal = (signals: Queue.Queue<string>) =>
  Queue.take(signals).pipe(Effect.map(toSignal));

const raceFirst = (branch = takeSignal) =>
  createRetryMachine(
    Signals.use((signals) =>
      Effect.raceFirst(
        branch(signals),
        Effect.sleep('4 seconds').pipe(Effect.as({ kind: 'deadline' as const }))
      )
    )
  );

const timeoutOption = createRetryMachine(
  Signals.use((signals) =>
    Queue.take(signals).pipe(
      Effect.timeoutOption('4 seconds'),
      Effect.map(
        Option.match({
          onNone: () => ({ kind: 'deadline' as const }),
          onSome: toSignal
        })
      )
    )
  )
);

const withAfter = createRetryMachine(Signals.use(takeSignal), 'after');

// The task keeps working for a second after the take.
const takeThenSave = (signals: Queue.Queue<string>) =>
  Queue.take(signals).pipe(
    Effect.flatMap((signal) =>
      Effect.sleep('1 second').pipe(Effect.as(toSignal(signal)))
    )
  );

/**
 * A Clock whose sleeps end only when the test calls `wake`, so each test
 * decides the exact step in which a deadline or a timer ends.
 */
function createManualClock(clock: Clock.Clock) {
  const pending = new Set<{ millis: number; resume: () => void }>();
  const sleeping = (millis: number) =>
    [...pending].filter((sleep) => sleep.millis === millis);
  const manual: Clock.Clock = Object.create(clock);
  manual.sleep = (duration) =>
    Effect.callback<void>((resume) => {
      const sleep = {
        millis: Duration.toMillis(duration),
        resume: () => resume(Effect.void)
      };
      pending.add(sleep);
      return Effect.sync(() => void pending.delete(sleep));
    });
  return {
    clock: manual,
    /** Waits until a sleep of `millis` has started. */
    started: (millis: number) =>
      Effect.gen(function* () {
        for (let i = 0; i < 100 && !sleeping(millis).length; i++) {
          yield* Effect.yieldNow;
        }
        expect(sleeping(millis)).not.toHaveLength(0);
      }),
    /** Ends every pending sleep of `millis` in the calling step. */
    wake: (millis: number) => {
      for (const sleep of sleeping(millis)) {
        pending.delete(sleep);
        sleep.resume();
      }
    }
  };
}

function run<A, E>(
  effect: (
    manual: ReturnType<typeof createManualClock>
  ) => Effect.Effect<A, E, Signals | Scope.Scope>
) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const signals = yield* Queue.unbounded<string>();
      const manual = createManualClock(yield* Clock.Clock);
      return yield* effect(manual).pipe(
        Effect.provideService(Signals, signals),
        Effect.provideService(Clock.Clock, manual.clock)
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer()))
  );
}

// `Queue.offerUnsafe` wakes a waiting take on a later scheduler step.
// `Queue.flushUnsafe` wakes it at once, so the take removes the item in the
// same step.
const offerAndTake = (signals: Queue.Queue<string>) => {
  Queue.offerUnsafe(signals, 'config changed');
  Queue.flushUnsafe(signals);
};

// Each schedule runs in one synchronous step, with nothing in between.
const schedules = {
  'taken before the timer ends': (
    signals: Queue.Queue<string>,
    wake: () => void
  ) => {
    offerAndTake(signals);
    wake();
  },
  'offered before the timer ends and taken after': (
    signals: Queue.Queue<string>,
    wake: () => void
  ) => {
    Queue.offerUnsafe(signals, 'config changed');
    wake();
  },
  'offered and taken after the timer ends': (
    signals: Queue.Queue<string>,
    wake: () => void
  ) => {
    wake();
    offerAndTake(signals);
  }
};

// Either winner is valid at a tie. Taking the signal and then reporting the
// deadline is not, and neither is an `after` timer that drops it.
const kept = { value: 'retryLater', size: 1 };
const lost = { value: 'retryLater', size: 0 };
const won = { value: 'retryNow', size: 0 };

describe('a signal that arrives at the instant the timer ends', () => {
  it.each([
    ['Effect.raceFirst', raceFirst(), 'taken before the timer ends', won],
    [
      'Effect.raceFirst',
      raceFirst(),
      'offered before the timer ends and taken after',
      kept
    ],
    [
      'Effect.raceFirst',
      raceFirst(),
      'offered and taken after the timer ends',
      kept
    ],
    [
      'Effect.timeoutOption',
      timeoutOption,
      'offered before the timer ends and taken after',
      kept
    ],
    ['an `after` timer', withAfter, 'taken before the timer ends', won],
    [
      'an `after` timer',
      withAfter,
      'offered before the timer ends and taken after',
      lost
    ],
    [
      'an `after` timer',
      withAfter,
      'offered and taken after the timer ends',
      lost
    ]
  ] as const)(
    'with %s, when the signal is %s, reaches %o',
    async (_timer, machine, schedule, expected) => {
      const result = await run(({ started, wake }) =>
        Effect.gen(function* () {
          const signals = yield* Signals;
          const actor = yield* createEffectActor(machine);
          yield* started(4_000);
          yield* Effect.sync(() =>
            schedules[schedule](signals, () => wake(4_000))
          );
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return { value: snapshot.value, size: yield* Queue.size(signals) };
        })
      );
      expect(result).toEqual(expected);
    }
  );
});

describe('a task that keeps working after the take', () => {
  it.each([
    [
      'an `after` timer',
      createRetryMachine(Signals.use(takeThenSave), 'after'),
      'timer',
      'retryLater'
    ],
    [
      'the deadline inside the task',
      raceFirst(takeThenSave),
      'timer',
      'retryLater'
    ],
    [
      'another event that exits the invoking state',
      raceFirst(takeThenSave),
      'cancel',
      'cancelled'
    ]
  ] as const)('loses the signal to %s', async (_exit, machine, exit, value) => {
    const result = await run(({ started, wake }) =>
      Effect.gen(function* () {
        const signals = yield* Signals;
        const actor = yield* createEffectActor(machine);
        yield* started(4_000);
        offerAndTake(signals);
        // The task has the signal and is saving it.
        yield* started(1_000);
        if (exit === 'cancel') actor.send({ type: 'cancel' });
        else wake(4_000);
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return { value: snapshot.value, size: yield* Queue.size(signals) };
      })
    );
    expect(result).toEqual({ value, size: 0 });
  });
});

// Core async logic under createEffectActor, with a take that is withdrawn
// when its abort signal aborts.
describe('core async logic under createEffectActor', () => {
  function createTimeoutMachine() {
    const items: string[] = [];
    let waiter: ((value: string) => void) | undefined;
    const offer = (value: string) =>
      waiter ? waiter(value) : items.push(value);
    const machine = createMachine({
      initial: 'waiting',
      states: {
        waiting: {
          invoke: {
            src: createAsyncLogic({
              timeout: 4_000,
              run: ({ signal }) =>
                new Promise<string>((resolvePromise, reject) => {
                  waiter = resolvePromise;
                  signal.addEventListener('abort', () => {
                    waiter = undefined;
                    reject(signal.reason);
                  });
                })
            }),
            onDone: { target: 'retryNow' },
            onError: ({ event }) => ({
              target:
                event.error instanceof TimeoutError ? 'retryLater' : 'failed'
            })
          }
        },
        retryNow: { type: 'final' },
        retryLater: { type: 'final' },
        failed: { type: 'final' }
      }
    });
    return { machine, items, offer };
  }

  afterEach(() => {
    vi.useRealTimers();
  });

  it('keeps a message that arrives before the machine handles an async logic timeout', async () => {
    // Async logic arms its own timeout on the global setTimeout, which
    // vitest fakes here.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const { machine, items, offer } = createTimeoutMachine();
    const result = await run(() =>
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        // The child starts on the actor's fiber. Wait until it has armed
        // its deadline, so the deadline fires before the offer.
        for (let i = 0; i < 100 && vi.getTimerCount() === 0; i++) {
          yield* Effect.yieldNow;
        }
        let valueAtOffer: unknown;
        setTimeout(() => {
          valueAtOffer = actor.getSnapshot().value;
          offer('config changed');
        }, 4_000);
        // Both timers fire in one task, so the message arrives after the
        // deadline aborted the take and before the machine handles it.
        vi.advanceTimersByTime(4_000);
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return { value: snapshot.value, valueAtOffer };
      })
    );
    expect(result).toEqual({ value: 'retryLater', valueAtOffer: 'waiting' });
    expect(items).toEqual(['config changed']);
  });

  it('does not advance an async logic timeout with TestClock', async () => {
    const value = await Effect.runPromise(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(createTimeoutMachine().machine);
        yield* TestClock.adjust('5 seconds');
        // Give a timeout that ran on TestClock time a chance to reach the
        // machine.
        yield* Effect.promise(
          () => new Promise((resolvePromise) => setTimeout(resolvePromise, 20))
        );
        return actor.getSnapshot().value;
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer()))
    );
    expect(value).toBe('waiting');
  });
});
