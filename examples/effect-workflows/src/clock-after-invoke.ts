import { Effect } from 'effect';
import { TestClock } from 'effect/testing';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor
} from '@xstate/effect';

let attempts = 0;
const connect = createEffectLogic(
  Effect.gen(function* () {
    // The yield stands in for real I/O. It is why the test below must wait
    // for `backoff` before it advances the clock.
    yield* Effect.yieldNow;
    attempts++;
    if (attempts < 2) {
      return yield* Effect.fail(new Error('Connection refused'));
    }
    return 'connected';
  })
);

const connectionMachine = setupEffect({ actors: { connect } }).createMachine({
  initial: 'idle',
  states: {
    idle: { on: { CONNECT: { target: 'connecting' } } },
    connecting: {
      invoke: {
        src: 'connect',
        onDone: { target: 'connected' },
        onError: { target: 'backoff' }
      }
    },
    backoff: { after: { 3000: { target: 'connecting' } } },
    connected: {}
  }
});

const test = Effect.gen(function* () {
  const actor = yield* createEffectActor(connectionMachine);
  actor.send({ type: 'CONNECT' });
  // Wait for the first failure to start the backoff timer. Without this wait
  // the clock moves first and the last `waitFor` never returns.
  yield* waitFor(actor, (s) => s.matches('backoff'));
  yield* TestClock.adjust('3 seconds');
  const snapshot = yield* waitFor(actor, (s) => s.matches('connected'));
  return snapshot.value;
});

export const result = await Effect.runPromise(
  test.pipe(Effect.scoped, Effect.provide(TestClock.layer()))
);
console.log(result); // 'connected', after one 3-second backoff
