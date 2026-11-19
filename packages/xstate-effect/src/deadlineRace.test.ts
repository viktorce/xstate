import {
  Clock,
  Context,
  Deferred,
  Effect,
  Fiber,
  Option,
  Queue,
  Scope
} from 'effect';
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

const retryMachine = createRetryMachine(
  Signals.use((signals) =>
    Effect.raceFirst(
      Queue.take(signals).pipe(
        Effect.map((signal) => ({ kind: 'signal' as const, signal }))
      ),
      Effect.sleep('4 seconds').pipe(Effect.as({ kind: 'deadline' as const }))
    )
  )
);

const timeoutMachine = createRetryMachine(
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

const afterMachine = createAfterMachine(
  Signals.use((signals) => Queue.take(signals))
);

function run<A, E>(effect: Effect.Effect<A, E, Signals | Scope.Scope>) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const signals = yield* Queue.unbounded<string>();
      return yield* effect.pipe(Effect.provideService(Signals, signals));
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer()))
  );
}

// Records each sleep started on `clock` under `label`. `registered` waits for
// the first one. `doneUnsafe` resumes a waiting fiber synchronously, before
// the test clock's sleep has registered. `registered` then yields, and the
// sleeping fiber registers its sleep before the waiter resumes.
function recordSleeps(clock: Clock.Clock, label: string, order: string[]) {
  const started = Deferred.makeUnsafe<void>();
  const recording: Clock.Clock = Object.create(clock);
  recording.sleep = (duration) =>
    Effect.suspend(() => {
      order.push(label);
      Deferred.doneUnsafe(started, Effect.void);
      return clock.sleep(duration);
    });
  return {
    clock: recording,
    registered: Deferred.await(started).pipe(Effect.andThen(Effect.yieldNow))
  };
}

describe('racing a deadline inside the task, compared with an `after` timer', () => {
  it('reaches retryLater and leaves a later signal in the queue', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const deadline = recordSleeps(yield* Clock.Clock, 'deadline', []);
        const actor = yield* createEffectActor(retryMachine).pipe(
          Effect.provideService(Clock.Clock, deadline.clock)
        );
        yield* deadline.registered;
        yield* TestClock.adjust('4 seconds');
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        yield* Queue.offer(signals, 'config changed');
        return { value: snapshot.value, size: yield* Queue.size(signals) };
      })
    );
    expect(result).toEqual({ value: 'retryLater', size: 1 });
  });

  it('with Effect.timeoutOption, reaches retryLater and leaves a later signal in the queue', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const deadline = recordSleeps(yield* Clock.Clock, 'deadline', []);
        const actor = yield* createEffectActor(timeoutMachine).pipe(
          Effect.provideService(Clock.Clock, deadline.clock)
        );
        yield* deadline.registered;
        yield* TestClock.adjust('4 seconds');
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        yield* Queue.offer(signals, 'config changed');
        return { value: snapshot.value, size: yield* Queue.size(signals) };
      })
    );
    expect(result).toEqual({ value: 'retryLater', size: 1 });
  });

  it.each([
    ['Effect.raceFirst', retryMachine],
    ['Effect.timeoutOption', timeoutMachine]
  ] as const)(
    'with %s, reaches retryNow and empties the queue when the signal comes first',
    async (_description, machine) => {
      const result = await run(
        Effect.gen(function* () {
          const signals = yield* Signals;
          const deadline = recordSleeps(yield* Clock.Clock, 'deadline', []);
          const actor = yield* createEffectActor(machine).pipe(
            Effect.provideService(Clock.Clock, deadline.clock)
          );
          yield* deadline.registered;
          yield* TestClock.adjust('3 seconds');
          yield* Queue.offer(signals, 'config changed');
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return { value: snapshot.value, size: yield* Queue.size(signals) };
        })
      );
      expect(result).toEqual({ value: 'retryNow', size: 0 });
    }
  );

  it.each([
    [
      'the signal timer was armed first',
      true,
      ['offer', 'deadline'],
      'retryNow'
    ],
    ['the deadline was armed first', false, ['deadline', 'offer'], 'retryLater']
  ] as const)(
    'never consumes a signal it does not report when %s',
    async (_description, signalFirst, expectedOrder, expectedValue) => {
      const result = await run(
        Effect.gen(function* () {
          const signals = yield* Signals;
          const clock = yield* Clock.Clock;
          const order: string[] = [];
          const deadline = recordSleeps(clock, 'deadline', order);
          const offer = recordSleeps(clock, 'offer', order);
          let offered = 0;
          // Start each contender and wait until its sleep is registered, so
          // the second one starts only after the first is armed.
          const armOffer = Effect.forkChild(
            Effect.sleep('4 seconds').pipe(
              Effect.andThen(
                Effect.suspend(() => {
                  offered++;
                  return Queue.offer(signals, 'config changed');
                })
              ),
              Effect.provideService(Clock.Clock, offer.clock)
            )
          ).pipe(Effect.tap(() => offer.registered));
          const armActor = createEffectActor(retryMachine).pipe(
            Effect.provideService(Clock.Clock, deadline.clock),
            Effect.tap(() => deadline.registered)
          );
          // `Effect.all` runs the struct's effects in key order.
          const { offerFiber, actor } = yield* Effect.all(
            signalFirst
              ? { offerFiber: armOffer, actor: armActor }
              : { actor: armActor, offerFiber: armOffer }
          );
          yield* TestClock.adjust('4 seconds');
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          yield* Fiber.join(offerFiber);
          return {
            order,
            value: snapshot.value,
            offered,
            size: yield* Queue.size(signals)
          };
        })
      );
      expect(result.order).toEqual(expectedOrder);
      expect(result.offered).toBe(1);
      // Either outcome is valid at a tie. Taking the signal and then
      // reporting the deadline is not.
      expect(['retryNow', 'retryLater']).toContain(result.value);
      expect(result.size).toBe(result.value === 'retryNow' ? 0 : 1);
      // The test clock wakes sleeps that end together in registration order
      // and lets each woken fiber settle first, so the first one armed wins.
      expect(result.value).toBe(expectedValue);
    }
  );

  // The same schedule loses the signal with `after` and keeps it with the
  // deadline inside the task. In `retryMachine` the wrapped sleep is the
  // raced `Effect.sleep`, not a state timer.
  it.each([
    ['an `after` timer', afterMachine, 0],
    ['the deadline inside the task', retryMachine, 1]
  ] as const)(
    'with %s, reaches retryLater when the signal is offered as the sleep ends',
    async (_description, machine, expectedSize) => {
      const result = await run(
        Effect.gen(function* () {
          const signals = yield* Signals;
          const clock = yield* Clock.Clock;
          // Offer the signal when the sleep ends, before the sleeping fiber
          // reports to the machine.
          let offered = 0;
          const timerClock: Clock.Clock = Object.create(clock);
          timerClock.sleep = (duration) =>
            Effect.tap(clock.sleep(duration), () => {
              offered++;
              return Queue.offer(signals, 'config changed');
            });
          const actor = yield* createEffectActor(machine).pipe(
            Effect.provideService(Clock.Clock, timerClock)
          );
          yield* TestClock.adjust('4 seconds');
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return {
            value: snapshot.value,
            offered,
            size: yield* Queue.size(signals)
          };
        })
      );
      expect(result).toEqual({
        value: 'retryLater',
        offered: 1,
        size: expectedSize
      });
    }
  );

  // The docs warn against a raced branch that keeps working after the take.
  // The signal arrives at 3.5 seconds, so the deadline lands inside that work.
  it.each([
    [
      'maps the signal synchronously',
      'retryNow',
      (signal: string) => Effect.succeed({ kind: 'signal' as const, signal })
    ],
    [
      'keeps working for a second',
      'retryLater',
      (signal: string) =>
        Effect.sleep('1 second').pipe(
          Effect.as({ kind: 'signal' as const, signal })
        )
    ]
  ] as const)(
    'with a take branch that %s after the take, empties the queue and reaches %s',
    async (_description, expectedValue, afterTake) => {
      const machine = createRetryMachine(
        Signals.use((signals) =>
          Effect.raceFirst(
            Queue.take(signals).pipe(Effect.flatMap(afterTake)),
            Effect.sleep('4 seconds').pipe(
              Effect.as({ kind: 'deadline' as const })
            )
          )
        )
      );
      const result = await run(
        Effect.gen(function* () {
          const signals = yield* Signals;
          const deadline = recordSleeps(yield* Clock.Clock, 'deadline', []);
          const actor = yield* createEffectActor(machine).pipe(
            Effect.provideService(Clock.Clock, deadline.clock)
          );
          yield* deadline.registered;
          yield* TestClock.adjust('3500 millis');
          const offered = yield* Queue.offer(signals, 'config changed');
          yield* TestClock.adjust('1 second');
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return {
            value: snapshot.value,
            offered,
            size: yield* Queue.size(signals)
          };
        })
      );
      expect(result).toEqual({ value: expectedValue, offered: true, size: 0 });
    }
  );

  // The first loss path in the docs: the task takes the signal at 3.5
  // seconds and is still working when the `after` timer fires at 4 seconds.
  it('with an `after` timer, drops a signal the task took and kept working on', async () => {
    const machine = createAfterMachine(
      Signals.use((signals) =>
        Queue.take(signals).pipe(Effect.tap(() => Effect.sleep('1 second')))
      )
    );
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const timer = recordSleeps(yield* Clock.Clock, 'after', []);
        const actor = yield* createEffectActor(machine).pipe(
          Effect.provideService(Clock.Clock, timer.clock)
        );
        yield* timer.registered;
        yield* TestClock.adjust('3500 millis');
        yield* Queue.offer(signals, 'config changed');
        yield* TestClock.adjust('1 second');
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return { value: snapshot.value, size: yield* Queue.size(signals) };
      })
    );
    expect(result).toEqual({ value: 'retryLater', size: 0 });
  });

  it('still drops a signal when another event exits the invoking state', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const actor = yield* createEffectActor(retryMachine);
        yield* TestClock.adjust('1 second');
        // The size of 0 depends on Effect ordering. The offer releases the
        // waiting `Queue.take` before the machine's `waitForEvent` promise
        // hands it `cancel`, so the take has the signal when it is
        // interrupted. A failure here after an Effect upgrade can mean that
        // order changed, not that the docs are wrong.
        yield* Queue.offer(signals, 'config changed');
        actor.send({ type: 'cancel' });
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        yield* TestClock.adjust('4 seconds');
        return { value: snapshot.value, size: yield* Queue.size(signals) };
      })
    );
    expect(result).toEqual({ value: 'cancelled', size: 0 });
  });
});

// Core async logic children under createEffectActor, with a queue whose
// pending take is withdrawn when its abort signal aborts. The machine handles
// a timer later than the timer fires. The core guide says a deadline inside
// the logic keeps a message that arrives in that window, and `after` does not.
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
    const result = await run(
      Effect.gen(function* () {
        const clock = yield* Clock.Clock;
        const valuesAtOffer: unknown[] = [];
        let read = () => undefined as unknown;
        // The machine's `after` timer sleeps on Effect's Clock. When the
        // sleep ends, the timer fiber queues the timer's event in the same
        // step, and the message arrives in the microtask after that step.
        const timerClock: Clock.Clock = Object.create(clock);
        timerClock.sleep = (duration) =>
          Effect.tap(clock.sleep(duration), () =>
            Effect.sync(() =>
              queueMicrotask(() => {
                valuesAtOffer.push(read());
                messages.offer('config changed');
              })
            )
          );
        const actor = yield* createEffectActor(machine).pipe(
          Effect.provideService(Clock.Clock, timerClock)
        );
        read = () => actor.getSnapshot().value;
        yield* TestClock.adjust('4 seconds');
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return { value: snapshot.value, valuesAtOffer };
      })
    );
    expect(result).toEqual({
      value: 'retryLater',
      valuesAtOffer: ['waiting']
    });
    expect(messages.items).toEqual([]);
  });

  it.each([
    ['retryLater', 'after the deadline fires', false, ['config changed']],
    ['retryNow', 'before the deadline fires', true, []]
  ] as const)(
    'is not lost with an async logic timeout, reaching %s when it arrives %s',
    async (expectedValue, _description, messageFirst, expectedItems) => {
      // Async logic arms its own timeout through the actor system's clock.
      // Under createEffectActor that is the core default clock, which calls
      // the global setTimeout. Vitest fakes it here, and Effect's Clock does
      // not see it.
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
      const result = await run(
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
          // Fails here, not at the test timeout, if the child never arms it.
          expect(vi.getTimerCount()).toBeGreaterThanOrEqual(armedTimers);
          if (!messageFirst) offerAtDeadline();
          // Moving Effect's test clock past the deadline leaves the timeout
          // armed. Both the deadline and the offer are still pending.
          yield* TestClock.adjust('5 seconds');
          expect(vi.getTimerCount()).toBe(2);
          expect(read()).toBe('waiting');
          if (messageFirst) {
            // Settling the take before the deadline fires lets the child
            // report it and clear the deadline.
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
      expect(result).toEqual({
        value: expectedValue,
        valuesAtOffer: ['waiting']
      });
      expect(messages.items).toEqual(expectedItems);
    }
  );
});
