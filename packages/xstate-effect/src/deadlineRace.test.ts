import { Clock, Context, Effect, Queue, Scope } from 'effect';
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

describe('racing a deadline inside the task', () => {
  it('reaches retryLater and leaves a later signal in the queue', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const actor = yield* createEffectActor(retryMachine);
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
        const actor = yield* createEffectActor(retryMachine);
        yield* TestClock.adjust('3 seconds');
        yield* Queue.offer(signals, 'config changed');
        const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
        return { value: snapshot.value, size: yield* Queue.size(signals) };
      })
    );
    expect(result).toEqual({ value: 'retryNow', size: 0 });
  });

  it.each([
    ['the signal timer was armed first', true],
    ['the deadline was armed first', false]
  ] as const)(
    'never consumes a signal it does not report when %s',
    async (_description, signalFirst) => {
      const result = await run(
        Effect.gen(function* () {
          const signals = yield* Signals;
          const offerAtDeadline = Effect.forkChild(
            Effect.sleep('4 seconds').pipe(
              Effect.andThen(Queue.offer(signals, 'config changed'))
            )
          );
          if (signalFirst) yield* offerAtDeadline;
          const actor = yield* createEffectActor(retryMachine);
          if (!signalFirst) yield* offerAtDeadline;
          yield* TestClock.adjust('4 seconds');
          const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
          return { value: snapshot.value, size: yield* Queue.size(signals) };
        })
      );
      // Either outcome is valid at a tie. Taking the signal and then
      // reporting the deadline is not.
      expect(['retryNow', 'retryLater']).toContain(result.value);
      expect(result.size).toBe(result.value === 'retryNow' ? 0 : 1);
    }
  );

  it('drops a signal taken after the timer event is queued', async () => {
    const result = await run(
      Effect.gen(function* () {
        const signals = yield* Signals;
        const clock = yield* Clock.Clock;
        // Offer the signal when the `after` timer's sleep ends, before the
        // timer offers its event to the machine.
        let offered = 0;
        const timerClock: Clock.Clock = Object.create(clock);
        timerClock.sleep = (duration) =>
          Effect.tap(clock.sleep(duration), () => {
            offered++;
            return Queue.offer(signals, 'config changed');
          });
        const actor = yield* createEffectActor(afterMachine).pipe(
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
    expect(result).toEqual({ value: 'retryLater', offered: 1, size: 0 });
  });

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
