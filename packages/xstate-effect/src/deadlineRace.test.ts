import { Clock, Context, Duration, Effect, Option, Queue, Scope } from 'effect';
import { TestClock } from 'effect/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createAsyncLogic, createMachine, TimeoutError } from 'xstate';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor,
  type EffectActor
} from './index.ts';

class Signals extends Context.Service<Signals, Queue.Queue<string>>()(
  '@test/Signals'
) {}

type Outcome = { kind: 'signal'; signal: string } | { kind: 'deadline' };

const toSignal = (signal: string) => ({ kind: 'signal' as const, signal });

// The machine from examples/effect-workflows/src/deadline.ts and, for the
// pattern the docs warn about, an `after` timer.
function createRetryMachine(
  task: Effect.Effect<Outcome, never, Signals>,
  timer?: 'after'
) {
  const nextSignal = createEffectLogic(task);
  return setupEffect({ actors: { nextSignal } }).createMachine({
    context: { signal: null as string | null },
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'nextSignal',
          onDone: ({ event }) =>
            event.output.kind === 'signal'
              ? { target: 'retryNow', context: { signal: event.output.signal } }
              : { target: 'retryLater' }
        },
        after: timer ? { 4_000: { target: 'retryLater' } } : {}
      },
      retryNow: { type: 'final' },
      retryLater: { type: 'final' }
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

const machines = {
  'Effect.raceFirst': raceFirst(),
  'Effect.timeoutOption': timeoutOption,
  'an `after` timer': createRetryMachine(Signals.use(takeSignal), 'after')
};

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
    }).pipe(
      Effect.scoped,
      // TestClock fixes the time the actor reads for timer deadlines, and the
      // manual clock built on it replaces only `sleep`.
      Effect.provide(TestClock.layer())
    )
  );
}

const outcome = (
  actor: EffectActor<ReturnType<typeof createRetryMachine>>,
  signals: Queue.Queue<string>
) =>
  Effect.gen(function* () {
    const { value, context } = yield* waitFor(
      actor,
      (s) => s.status === 'done'
    );
    return { value, signal: context.signal, size: yield* Queue.size(signals) };
  });

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
const outcomes = {
  'reports the signal': {
    value: 'retryNow',
    signal: 'config changed',
    size: 0
  },
  'keeps the signal in the queue': {
    value: 'retryLater',
    signal: null,
    size: 1
  },
  'loses the signal': { value: 'retryLater', signal: null, size: 0 }
};

describe('a signal that arrives at the instant the timer ends', () => {
  it.each([
    ['Effect.raceFirst', 'taken before the timer ends', 'reports the signal'],
    [
      'Effect.raceFirst',
      'offered before the timer ends and taken after',
      'keeps the signal in the queue'
    ],
    [
      'Effect.raceFirst',
      'offered and taken after the timer ends',
      'keeps the signal in the queue'
    ],
    [
      'Effect.timeoutOption',
      'offered before the timer ends and taken after',
      'keeps the signal in the queue'
    ],
    ['an `after` timer', 'taken before the timer ends', 'reports the signal'],
    [
      'an `after` timer',
      'offered before the timer ends and taken after',
      'loses the signal'
    ],
    [
      'an `after` timer',
      'offered and taken after the timer ends',
      'loses the signal'
    ]
  ] as const)(
    'with %s, when the signal is %s, %s',
    async (timer, schedule, expected) => {
      const result = await run(({ started, wake }) =>
        Effect.gen(function* () {
          const signals = yield* Signals;
          const actor = yield* createEffectActor(machines[timer]);
          yield* started(4_000);
          yield* Effect.sync(() =>
            schedules[schedule](signals, () => wake(4_000))
          );
          return yield* outcome(actor, signals);
        })
      );
      expect(result).toEqual(outcomes[expected]);
    }
  );
});

it('loses the signal to the deadline inside a task that keeps working after the take', async () => {
  // The task saves the signal for a second after the take.
  const machine = raceFirst((signals) =>
    Queue.take(signals).pipe(
      Effect.flatMap((signal) =>
        Effect.sleep('1 second').pipe(Effect.as(toSignal(signal)))
      )
    )
  );
  const result = await run(({ started, wake }) =>
    Effect.gen(function* () {
      const signals = yield* Signals;
      const actor = yield* createEffectActor(machine);
      yield* started(4_000);
      offerAndTake(signals);
      yield* started(1_000);
      wake(4_000);
      return yield* outcome(actor, signals);
    })
  );
  expect(result).toEqual(outcomes['loses the signal']);
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

  // Async logic arms its own timeout on the global setTimeout, which vitest
  // fakes here. Effect schedules its own steps with setImmediate, which stays
  // real.
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // The child starts on the actor's fiber. Waits until it has armed its
  // deadline.
  const armed = Effect.gen(function* () {
    for (let i = 0; i < 100 && vi.getTimerCount() === 0; i++) {
      yield* Effect.yieldNow;
    }
    expect(vi.getTimerCount()).toBe(1);
  });

  it('keeps a message that arrives before the machine handles an async logic timeout', async () => {
    const { machine, items, offer } = createTimeoutMachine();
    const result = await run(() =>
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        // The deadline is armed first, so it fires before the offer.
        yield* armed;
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

  it('runs an async logic timeout on the global timers, not on TestClock', async () => {
    const values = await Effect.runPromise(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(createTimeoutMachine().machine);
        yield* armed;
        yield* TestClock.adjust('5 seconds');
        for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
        const afterTestClock = actor.getSnapshot().value;
        vi.advanceTimersByTime(4_000);
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return [afterTestClock, snapshot.value];
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer()))
    );
    expect(values).toEqual(['waiting', 'retryLater']);
  });
});
