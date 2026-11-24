import { Context, Effect, Queue } from 'effect';
import { TestClock } from 'effect/testing';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor
} from '@xstate/effect';

class Rollout extends Context.Service<
  Rollout,
  { readonly status: Effect.Effect<'ready' | 'not ready'> }
>()('@app/Rollout') {}

const poll = createEffectLogic(
  Effect.gen(function* () {
    const rollout = yield* Rollout;
    while ((yield* rollout.status) === 'not ready') {
      yield* Effect.sleep('5 seconds');
    }
    return 'ready';
  })
);

const rolloutMachine = setupEffect({ actors: { poll } }).createMachine({
  initial: 'idle',
  states: {
    idle: { on: { DEPLOY: { target: 'polling' } } },
    polling: { invoke: { src: 'poll', onDone: { target: 'live' } } },
    live: { type: 'final' }
  }
});

const test = Effect.gen(function* () {
  const sleeping = yield* Queue.unbounded<void>();
  let calls = 0;
  const status = Effect.gen(function* () {
    // The host timer stands in for real I/O.
    yield* Effect.promise(
      () => new Promise((resolve) => setTimeout(resolve, 5))
    );
    if (++calls === 3) return 'ready' as const;
    // The offer marks the moment before the poll sleeps.
    yield* Queue.offer(sleeping, undefined);
    return 'not ready' as const;
  });
  const actor = yield* createEffectActor(rolloutMachine).pipe(
    Effect.provideService(Rollout, { status })
  );
  actor.send({ type: 'DEPLOY' });
  // Without this take the clock moves first and `waitFor` never returns.
  yield* Queue.take(sleeping);
  yield* TestClock.adjust('5 seconds');
  yield* Queue.take(sleeping);
  yield* TestClock.adjust('5 seconds');
  const snapshot = yield* waitFor(actor, (s) => s.matches('live'));
  return snapshot.value;
});

export const result = await Effect.runPromise(
  test.pipe(Effect.scoped, Effect.provide(TestClock.layer()))
);
console.log(result); // 'live', after two 5-second sleeps
