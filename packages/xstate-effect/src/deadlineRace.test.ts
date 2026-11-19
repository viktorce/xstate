import { Clock, Context, Deferred, Effect, Fiber, Queue, Scope } from 'effect';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vitest';
import { types } from 'xstate';
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

const nextSignal = createEffectLogic(
  Signals.use((signals) =>
    Effect.raceFirst(
      Queue.take(signals).pipe(
        Effect.map((signal) => ({ kind: 'signal' as const, signal }))
      ),
      Effect.sleep('4 seconds').pipe(Effect.as({ kind: 'deadline' as const }))
    )
  )
);

const retryMachine = setupEffect({ actors: { nextSignal } }).createMachine({
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

// The pattern the docs warn about: the timer and the task race as two events.
const takeSignal = createEffectLogic(
  Signals.use((signals) => Queue.take(signals))
);

const afterMachine = setupEffect({
  actors: { takeSignal }
}).createMachine({
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

describe('racing a deadline inside the task', () => {
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

  it('reaches retryNow and empties the queue when the signal comes first', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const deadline = recordSleeps(yield* Clock.Clock, 'deadline', []);
        const actor = yield* createEffectActor(retryMachine).pipe(
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
  });

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

  it('still drops a signal when another event exits the invoking state', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const actor = yield* createEffectActor(retryMachine);
        yield* TestClock.adjust('1 second');
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
