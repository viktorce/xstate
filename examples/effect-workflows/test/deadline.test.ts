import { Effect, Queue } from 'effect';
import { TestClock } from 'effect/testing';
import { createEffectActor, waitFor } from '@xstate/effect';
import { retryMachine, Signals } from '../src/deadline.ts';

it('reaches retryLater with no signal when none arrives before the deadline', async () => {
  const program = Effect.gen(function* () {
    const actor = yield* createEffectActor(retryMachine).pipe(
      Effect.provideService(Signals, yield* Queue.unbounded<string>())
    );
    yield* TestClock.adjust('4 seconds');
    const { value, context } = yield* waitFor(
      actor,
      (s) => s.status === 'done'
    );
    return { value, signal: context.signal };
  });
  const result = await Effect.runPromise(
    program.pipe(Effect.scoped, Effect.provide(TestClock.layer()))
  );
  expect(result).toEqual({ value: 'retryLater', signal: null });
});
