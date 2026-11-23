import { Effect, Queue } from 'effect';
import { TestClock } from 'effect/testing';
import { createEffectActor, waitFor } from '@xstate/effect';
import { retryMachine, Signals } from '../src/deadline.ts';

it('reaches retryLater when no signal arrives before the deadline', async () => {
  const program = Effect.gen(function* () {
    const actor = yield* createEffectActor(retryMachine).pipe(
      Effect.provideService(Signals, yield* Queue.unbounded<string>())
    );
    yield* TestClock.adjust('4 seconds');
    const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
    return snapshot.value;
  });
  const value = await Effect.runPromise(
    program.pipe(Effect.scoped, Effect.provide(TestClock.layer()))
  );
  expect(value).toBe('retryLater');
});
