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
  // Like real I/O, an attempt takes at least one scheduler turn.
  Effect.yieldNow.pipe(
    Effect.andThen(
      Effect.suspend(() => {
        attempts++;
        return attempts < 2
          ? Effect.fail(new Error('Connection refused'))
          : Effect.succeed('connected');
      })
    )
  )
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
  // The first attempt is still running. Wait until its failure starts the
  // backoff timer, then advance the clock.
  yield* waitFor(actor, (s) => s.matches('backoff'));
  yield* TestClock.adjust('3 seconds');
  const snapshot = yield* waitFor(actor, (s) => s.matches('connected'));
  return snapshot.value;
});

export const result = await Effect.runPromise(
  test.pipe(Effect.scoped, Effect.provide(TestClock.layer()))
);
console.log(result); // 'connected', after one 3-second backoff
