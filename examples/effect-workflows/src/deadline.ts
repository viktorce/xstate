import { Context, Effect, Queue } from 'effect';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor
} from '@xstate/effect';

export class Signals extends Context.Service<Signals, Queue.Queue<string>>()(
  '@app/Signals'
) {}

// One Effect owns both outcomes. When the deadline wins, the race interrupts
// the take before it removes anything from the queue.
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
  initial: 'waiting',
  states: {
    waiting: {
      invoke: {
        src: 'nextSignal',
        onDone: ({ event }) => ({
          target: event.output.kind === 'signal' ? 'retryNow' : 'retryLater'
        })
      }
    },
    retryNow: { type: 'final' },
    retryLater: { type: 'final' }
  }
});

const program = Effect.gen(function* () {
  const signals = yield* Queue.unbounded<string>();
  const actor = yield* createEffectActor(retryMachine).pipe(
    Effect.provideService(Signals, signals)
  );
  yield* Queue.offer(signals, 'config changed');
  const snapshot = yield* waitFor(actor, (s) => s.status === 'done');
  return snapshot.value;
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 'retryNow'
