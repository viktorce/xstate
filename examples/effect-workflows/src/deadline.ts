import { Context, Effect, Queue } from 'effect';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor
} from '@xstate/effect';

export class Messages extends Context.Service<Messages, Queue.Queue<string>>()(
  '@app/Messages'
) {}

// One Effect owns both outcomes. The take branch holds only the take and one
// Effect.map, so it finishes in the step that removes the item. When the
// deadline wins, the race interrupts the take before it removes anything.
const nextMessage = createEffectLogic(
  Messages.use((messages) =>
    Effect.raceFirst(
      Queue.take(messages).pipe(
        Effect.map((message) => ({ kind: 'message' as const, message }))
      ),
      Effect.sleep('4 seconds').pipe(Effect.as({ kind: 'deadline' as const }))
    )
  )
);

export const retryMachine = setupEffect({
  actors: { nextMessage }
}).createMachine({
  context: { message: null as string | null },
  initial: 'waiting',
  states: {
    waiting: {
      invoke: {
        src: 'nextMessage',
        onDone: ({ event }) =>
          event.output.kind === 'message'
            ? { target: 'retryNow', context: { message: event.output.message } }
            : { target: 'retryLater' }
      }
    },
    retryNow: { type: 'final' },
    retryLater: { type: 'final' }
  }
});

const program = Effect.gen(function* () {
  const messages = yield* Queue.unbounded<string>();
  const actor = yield* createEffectActor(retryMachine).pipe(
    Effect.provideService(Messages, messages)
  );
  yield* Queue.offer(messages, 'config changed');
  const { value, context } = yield* waitFor(actor, (s) => s.status === 'done');
  return { value, message: context.message };
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // { value: 'retryNow', message: 'config changed' }
