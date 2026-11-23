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

// The machine from examples/effect-workflows/src/deadline.ts, with an
// optional `cancel` event on the invoking state.
class Signals extends Context.Service<Signals, Queue.Queue<string>>()(
  '@test/Signals'
) {}

type Outcome = { kind: 'signal'; signal: string } | { kind: 'deadline' };

function createRetryMachine(task: Effect.Effect<Outcome, never, Signals>) {
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
        on: { cancel: { target: 'cancelled' } }
      },
      retryNow: { type: 'final' },
      retryLater: { type: 'final' },
      cancelled: { type: 'final' }
    }
  });
}

// The pattern the docs warn about: the timer and the task race as two events.
function createAfterMachine(task: Effect.Effect<string, never, Signals>) {
  const takeSignal = createEffectLogic(task);
  return setupEffect({ actors: { takeSignal } }).createMachine({
    initial: 'waiting',
    states: {
      waiting: {
        invoke: { src: 'takeSignal', onDone: { target: 'retryNow' } },
        after: { 4_000: { target: 'retryLater' } }
      },
      retryNow: { type: 'final' },
      retryLater: { type: 'final' }
    }
  });
}

const raceFirst = (
  afterTake: (signal: string) => Effect.Effect<Outcome> = (signal) =>
    Effect.succeed({ kind: 'signal', signal })
) =>
  createRetryMachine(
    Signals.use((signals) =>
      Effect.raceFirst(
        Queue.take(signals).pipe(Effect.flatMap(afterTake)),
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
          onSome: (signal) => ({ kind: 'signal' as const, signal })
        })
      )
    )
  )
);

// The task keeps working for a second after the take.
const takeThenSave = (signal: string) =>
  Effect.sleep('1 second').pipe(Effect.as({ kind: 'signal' as const, signal }));

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

describe('a signal that arrives at the instant the timer ends', () => {
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

  // Each row pins the winner, so both branches of the item check run.
  it.each([
    [
      'Effect.raceFirst',
      raceFirst(),
      'taken before the timer ends',
      'retryNow'
    ],
    [
      'Effect.raceFirst',
      raceFirst(),
      'offered before the timer ends and taken after',
      'retryLater'
    ],
    [
      'Effect.raceFirst',
      raceFirst(),
      'offered and taken after the timer ends',
      'retryLater'
    ],
    [
      'Effect.timeoutOption',
      timeoutOption,
      'taken before the timer ends',
      'retryNow'
    ],
    [
      'Effect.timeoutOption',
      timeoutOption,
      'offered before the timer ends and taken after',
      'retryLater'
    ],
    [
      'Effect.timeoutOption',
      timeoutOption,
      'offered and taken after the timer ends',
      'retryLater'
    ]
  ] as const)(
    'with the deadline raced by %s, does not lose a signal %s',
    async (_race, machine, schedule, winner) => {
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
      expect(result.value).toBe(winner);
      // Either outcome is valid at a tie. Taking the signal and then
      // reporting the deadline is not.
      expect(result.size).toBe(result.value === 'retryNow' ? 0 : 1);
    }
  );

  it.each([
    ['taken before the timer ends', 'retryNow'],
    ['offered before the timer ends and taken after', 'retryLater'],
    ['offered and taken after the timer ends', 'retryLater']
  ] as const)(
    'with an `after` timer, empties the queue when the signal is %s and reaches %s',
    async (schedule, expectedValue) => {
      const machine = createAfterMachine(
        Signals.use((signals) => Queue.take(signals))
      );
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
      // Reaching retryLater with an empty queue means the task took the
      // signal and the machine dropped it.
      expect(result).toEqual({ value: expectedValue, size: 0 });
    }
  );
});

describe('a task that keeps working after the take', () => {
  it.each([
    [
      'an `after` timer',
      createAfterMachine(
        Signals.use((signals) =>
          Queue.take(signals).pipe(Effect.tap(takeThenSave))
        )
      ),
      (_actor: { send(event: { type: 'cancel' }): void }, wake: () => void) =>
        wake(),
      'retryLater'
    ],
    [
      'the deadline inside the task',
      raceFirst(takeThenSave),
      (_actor: { send(event: { type: 'cancel' }): void }, wake: () => void) =>
        wake(),
      'retryLater'
    ],
    [
      'another event that exits the invoking state',
      raceFirst(takeThenSave),
      (actor: { send(event: { type: 'cancel' }): void }) =>
        actor.send({ type: 'cancel' }),
      'cancelled'
    ]
  ] as const)(
    'loses the signal to %s',
    async (_exit, machine, exit, expectedValue) => {
      const result = await run(({ started, wake }) =>
        Effect.gen(function* () {
          const signals = yield* Signals;
          const actor = yield* createEffectActor(machine);
          yield* started(4_000);
          offerAndTake(signals);
          // The task has the signal and is saving it.
          yield* started(1_000);
          exit(actor as never, () => wake(4_000));
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return { value: snapshot.value, size: yield* Queue.size(signals) };
        })
      );
      expect(result).toEqual({ value: expectedValue, size: 0 });
    }
  );
});

// Core async logic children under createEffectActor, with a queue whose
// pending take is withdrawn when its abort signal aborts. The core guide says
// a deadline inside the logic keeps a message that arrives before the machine
// handles the deadline, and `after` does not.
describe('a message that arrives before the machine handles the deadline', () => {
  function createMessageQueue() {
    const items: string[] = [];
    let waiters: Array<(value: string) => void> = [];
    return {
      items,
      offer(value: string) {
        const waiter = waiters.shift();
        if (waiter) waiter(value);
        else items.push(value);
      },
      take(signal: AbortSignal) {
        if (items.length) return Promise.resolve(items.shift()!);
        return new Promise<string>((resolvePromise, reject) => {
          const waiter = (value: string) => resolvePromise(value);
          signal.addEventListener('abort', () => {
            waiters = waiters.filter((candidate) => candidate !== waiter);
            reject(signal.reason);
          });
          waiters.push(waiter);
        });
      }
    };
  }

  afterEach(() => {
    vi.useRealTimers();
  });

  it('is lost with `after` and a take-only child', async () => {
    const messages = createMessageQueue();
    const machine = createMachine({
      initial: 'waiting',
      states: {
        waiting: {
          invoke: {
            src: createAsyncLogic({
              run: ({ signal }) => messages.take(signal)
            }),
            onDone: { target: 'retryNow' }
          },
          after: { 4_000: { target: 'retryLater' } }
        },
        retryNow: { type: 'final' },
        retryLater: { type: 'final' }
      }
    });
    const result = await run(({ started, wake }) =>
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* started(4_000);
        // The timer's event is in the mailbox when the message arrives.
        const valueAtOffer = yield* Effect.sync(() => {
          wake(4_000);
          messages.offer('config changed');
          return actor.getSnapshot().value;
        });
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return { value: snapshot.value, valueAtOffer };
      })
    );
    expect(result).toEqual({ value: 'retryLater', valueAtOffer: 'waiting' });
    expect(messages.items).toEqual([]);
  });

  // Each row pins the winner, so both branches of the item check run.
  it.each([
    ['the message timer was armed first', true, 'retryNow'],
    ['the deadline was armed first', false, 'retryLater']
  ] as const)(
    'is not lost with an async logic timeout when %s',
    async (_description, messageFirst, winner) => {
      // Async logic arms its own timeout on the global setTimeout, which
      // vitest fakes here.
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
      const messages = createMessageQueue();
      const machine = createMachine({
        initial: 'waiting',
        states: {
          waiting: {
            invoke: {
              src: createAsyncLogic({
                timeout: 4_000,
                run: ({ signal }) => messages.take(signal)
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
      const result = await run(() =>
        Effect.gen(function* () {
          const valuesAtOffer: unknown[] = [];
          let read = () => undefined as unknown;
          const offerAtDeadline = () =>
            setTimeout(() => {
              valuesAtOffer.push(read());
              messages.offer('config changed');
            }, 4_000);
          if (messageFirst) offerAtDeadline();
          const actor = yield* createEffectActor(machine);
          read = () => actor.getSnapshot().value;
          // The child starts on the actor's fiber. Wait until it has armed
          // its deadline, so the two timers fire in the order armed.
          const armedTimers = messageFirst ? 2 : 1;
          for (let i = 0; i < 100 && vi.getTimerCount() < armedTimers; i++) {
            yield* Effect.yieldNow;
          }
          expect(vi.getTimerCount()).toBe(armedTimers);
          if (!messageFirst) offerAtDeadline();
          // The Effect guide says TestClock does not advance the timeout.
          yield* TestClock.adjust('5 seconds');
          expect(read()).toBe('waiting');
          if (messageFirst) {
            // Each timer fires in its own task, so the child reports the
            // message before the deadline fires.
            yield* Effect.promise(() => vi.advanceTimersByTimeAsync(4_000));
          } else {
            // Both timers fire in one task, so the message arrives after the
            // deadline aborted the take and before the machine handles it.
            vi.advanceTimersByTime(4_000);
          }
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return { value: snapshot.value, valuesAtOffer };
        })
      );
      expect(result).toEqual({ value: winner, valuesAtOffer: ['waiting'] });
      // Either outcome is valid at a tie. Taking the message and then
      // reporting the deadline is not.
      expect(messages.items).toEqual(
        result.value === 'retryNow' ? [] : ['config changed']
      );
    }
  );
});
