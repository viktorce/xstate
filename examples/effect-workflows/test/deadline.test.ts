import { Effect, Fiber, Queue, Scope } from 'effect';
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

// A producer sleeps to the instant the deadline fires, armed before or after
// the task's deadline. Either winner is valid. Taking the message and then
// reporting the deadline is not.
it.each([
  ['the producer', true],
  ['the deadline', false]
] as const)(
  'keeps the message at a tie when %s is armed first',
  async (_first, producerFirst) => {
    const result = await runOnTestClock(
      Effect.gen(function* () {
        const messages = yield* Messages;
        const produce = Effect.sleep('4 seconds').pipe(
          Effect.andThen(Queue.offer(messages, 'config changed'))
        );
        const early = producerFirst ? yield* Effect.forkChild(produce) : null;
        const actor = yield* createEffectActor(retryMachine);
        // Lets the task start and arm its deadline.
        for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
        const producer = early ?? (yield* Effect.forkChild(produce));
        yield* TestClock.adjust('4 seconds');
        const reported = yield* finish(actor);
        yield* Fiber.join(producer);
        return { ...reported, size: yield* Queue.size(messages) };
      })
    );
    expect([
      { value: 'retryNow', message: 'config changed', size: 0 },
      { value: 'retryLater', message: null, size: 1 }
    ]).toContainEqual(result);
  }
);
