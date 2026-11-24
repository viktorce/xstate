import { Clock, Effect, Queue, Scope } from 'effect';
import { TestClock } from 'effect/testing';
import { createEffectActor, waitFor, type EffectActor } from '@xstate/effect';
import { Messages, retryMachine } from '../src/deadline.ts';

const runOnTestClock = <A, E>(
  program: Effect.Effect<A, E, Messages | Scope.Scope>
) =>
  Effect.gen(function* () {
    const messages = yield* Queue.unbounded<string>();
    return yield* program.pipe(Effect.provideService(Messages, messages));
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise);

// Waits for the machine to finish and reads what it reported.
const finish = (actor: EffectActor<typeof retryMachine>) =>
  Effect.gen(function* () {
    const { value, context } = yield* waitFor(
      actor,
      (s) => s.status === 'done'
    );
    return { value, message: context.message };
  });

it('reaches retryLater with no message when none arrives before the deadline', async () => {
  const result = await runOnTestClock(
    Effect.gen(function* () {
      const actor = yield* createEffectActor(retryMachine);
      yield* TestClock.adjust('4 seconds');
      return yield* finish(actor);
    })
  );
  expect(result).toEqual({ value: 'retryLater', message: null });
});

// The message reaches the waiting take and the deadline ends in one
// synchronous step. A Clock whose sleep ends only when the test calls
// `wakeDeadline` decides that step, and `Queue.flushUnsafe` hands the message
// to the take at once. The first of the two wins, and the loser keeps nothing.
const reported = { value: 'retryNow', message: 'config changed', size: 0 };
const kept = { value: 'retryLater', message: null, size: 1 };
it.each([
  ['the take gets the message first', true],
  ['the deadline ends first', false]
] as const)('keeps the message at a tie when %s', async (_order, takeFirst) => {
  const result = await runOnTestClock(
    Effect.gen(function* () {
      const messages = yield* Messages;
      let wakeDeadline: (() => void) | undefined;
      const clock: Clock.Clock = Object.create(yield* Clock.Clock);
      clock.sleep = () =>
        Effect.callback((resume) => {
          wakeDeadline = () => resume(Effect.void);
        });
      const actor = yield* createEffectActor(retryMachine).pipe(
        Effect.provideService(Clock.Clock, clock)
      );
      for (let i = 0; i < 100 && !wakeDeadline; i++) yield* Effect.yieldNow;
      const deliver = () => {
        Queue.offerUnsafe(messages, 'config changed');
        Queue.flushUnsafe(messages);
      };
      if (takeFirst) deliver();
      wakeDeadline!();
      if (!takeFirst) deliver();
      const reported = yield* finish(actor);
      return { ...reported, size: yield* Queue.size(messages) };
    })
  );
  expect(result).toEqual(takeFirst ? reported : kept);
});
