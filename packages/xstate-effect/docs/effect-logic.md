---
title: "XState Effect: Actor logic"
description: Use Effect tasks and streams as parts of an event-driven workflow.
---

Turn an Effect or Stream into [actor logic](../../../docs/actor-logic.md) with one of these functions. Start it with `createEffectActor`, or invoke it from a machine running under `createEffectActor`.

| Function | Use it for |
| --- | --- |
| `createEffectLogic` | A task whose result decides the next state. |
| `createEffectStreamLogic` | A changing value, such as upload progress. |
| `createEffectEventStreamLogic` | Events that drive a workflow, such as health checks. |

Each accepts a value, a function of actor arguments that returns the value, or a configuration object with `id`, `schemas`, `validator` and `effect` or `stream`.

## `createEffectLogic`

Use `createEffectLogic` for work the machine waits for: publishing a release, reserving inventory or building a report. The [quick start](quick-start.md) invokes a deployment task and handles success, failure and cancellation.

Here is a complete task with typed input and output:

<!-- example from examples/effect-workflows/src/task.ts -->

```ts
import { Effect, Schema } from 'effect';
import { createEffectActor, createEffectLogic, join } from '@xstate/effect';

const buildReport = createEffectLogic({
  schemas: { input: Schema.Struct({ orderIds: Schema.Array(Schema.String) }) },
  effect: ({ input, emit }) =>
    Effect.sync(() => {
      emit({ type: 'reportBuilt', count: input.orderIds.length });
      return { total: input.orderIds.length };
    })
});

const program = Effect.gen(function* () {
  const actor = yield* createEffectActor(buildReport, {
    input: { orderIds: ['order-1', 'order-2'] }
  });
  return yield* join(actor);
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // { total: 2 }
```

- The actor is `active` while the Effect runs.
- Success puts it in `done` and stores the value in `output`.
- Failure puts it in `error`; `join` and an invoking machine's `onError` receive the typed error.
- The task actor has no context of its own.

### Actor arguments

The function form receives `EffectSourceArgs<TInput>` once per actor start:

| Field | Description |
| --- | --- |
| `input` | The actor's input. |
| `self` | The actor's reference. |
| `system` | The actor system. |
| `emit` | Emits a notification for `actor.on(...)` and `emitted(actor)`. |

Input and output schemas are optional. Missing types are inferred from the function or Effect. See [schemas and actions](schemas-and-actions.md) for runtime validation.

### Cancellation and failure

Leaving an invoking state or stopping the actor interrupts the running Effect. Each task runs in its own scope:

- `Effect.acquireRelease` and `Effect.addFinalizer` clean up before a completed or failed task reports its outcome.
- Cancelling an invocation interrupts the task and runs its cleanup while the parent can continue in another state.
- Use `withActorScope` for resources that should stay open until the owning actor stops. See [resource lifetimes](actors.md#resource-lifetimes).

Use Effect's retry and timeout combinators inside the task.

<details>
<summary>How Effect exits map to actors</summary>

| Exit | Actor result |
| --- | --- |
| Success | `done`, with the success value as `output`. |
| Typed failure | `error`, with `EffectFailure<E>` carrying the original Cause. |
| Defect | `error`, with `EffectFailure<never>` carrying the defect Cause. |
| Interrupted because the actor stopped or the invoking state exited | Stopped, without an error. |
| Self-interruption inside the Effect | `error`, with `EffectFailure<never>` carrying the interruption Cause. |

`join` replays `EffectFailure.cause` with `Effect.failCause`, so defects and interruption retain their Effect semantics. `Effect.timeout` fails with `Cause.TimeoutError`. A lost `Effect.race` interrupts only the loser; the actor completes with the winner.

</details>

### Racing a deadline

A state timer can make the machine lose an item that an invoked task already took. An `after`, a state `timeout` or an invoke `timeout` is one event, and the task's result is another. If the timer's transition exits the invoking state before the machine handles the result, the machine stops the task. The item it took is lost, whether the task was still running or had already finished and queued its result behind the timer. Nothing reports the loss as an error. The core guide explains [what stops a child](../../../docs/invoke.md#racing-an-invocation-against-a-timer).

The root machine under `createEffectActor` receives events through one mailbox and handles them in the order they arrive. The timer's event can already be in the mailbox when the task takes an item from a `Queue`. That can happen even if the item was offered before the timer fired, because Effect can resume a waiting `Queue.take` on a later step. A step here is one run of a fiber, from when it resumes until it waits, yields or ends, together with the fibers it resumes synchronously, such as a race waiting for it. A nested machine receives its events directly and handles the timer when it fires, so this case does not apply to it.

When the result must not be lost to a timer, race the deadline inside the task, as the example below does. This needs Effect 4.0.2 or later. On 4.0.0 and 4.0.1, a waiting take under `Effect.raceFirst` or `Effect.timeoutOption` can lose an item to the deadline after it removed it.

The example's raced branch is `Queue.take` followed by one `Effect.map`. `Queue.take` removes an item and returns it in one step, and `Effect.raceFirst` interrupts the losing branch before the winner's step ends. That branch therefore either wins with the item or ends without removing it, even when the item arrives at the instant the deadline fires. This rests on Effect's implementation, with its default scheduler and `Scheduler.MaxOpsBeforeYield`.

A different branch keeps that property only if its take stays interruptible and the branch finishes in the step that takes the item. Each of these breaks it:

- An asynchronous step after the take, as in `Queue.take(q).pipe(Effect.flatMap(save))`, or a yield with `Effect.yieldNow`.
- More than about 2,000 Effect operations in the step that takes the item, counting those before the take, because the fiber then yields. A slow function inside one `Effect.map` counts as one operation.
- `Effect.uninterruptible` around the take. The task then reports no deadline until an item arrives, and loses that item.
- A child fiber forked with `Effect.forkChild` whose cleanup takes time, because the branch waits for that cleanup before it finishes.
- Code that ends the deadline from inside the step that takes the item, such as a hand-written `Clock` woken there or a `dispatcher.flush()` called there.

For an item that must survive in every case, use [reserve, then commit](../../../docs/invoke.md#reserve-then-commit).

<!-- example from examples/effect-workflows/src/deadline.ts -->

```ts
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

// One Effect owns both outcomes. The take branch is Queue.take and one
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
```

- `onDone` handles both outcomes, so no machine timer competes with the task. Another transition out of `waiting`, an explicit `enq.stop` of the task, or stopping the actor can still lose an item the task took.
- The consuming call must either finish with the item or leave it in the source when interrupted. `Queue.take` does. Other sources may not.
- `Effect.timeoutOption` runs the same race under the same conditions and returns `Option.none()` at the deadline.
- The deadline restarts in full when the actor is restored, because a running Effect task starts again from the beginning. A pending `after` timer keeps its original deadline. See [persisting and restoring](actors.md#persisting-and-restoring).
- Keep `after` and `timeout` for work that is safe to repeat or abandon.

The core guide covers the same race for `createAsyncLogic` in [racing an invocation against a timer](../../../docs/invoke.md#racing-an-invocation-against-a-timer). A `createAsyncLogic` `timeout` currently runs on the global timers rather than on Effect's `Clock`, so `TestClock` does not advance it.

## `createEffectStreamLogic`

Use `createEffectStreamLogic` when consumers need the latest value. For example, upload progress becomes the actor's snapshot context:

<!-- example from examples/effect-workflows/src/latest-stream.ts -->

```ts
import { Effect, Option, Stream } from 'effect';
import {
  createEffectActor,
  createEffectStreamLogic,
  snapshots
} from '@xstate/effect';

// Demo upload progress. Replace with your upload SDK's progress stream.
const uploadProgress = createEffectStreamLogic(Stream.make(0, 25, 60, 100));

const program = Effect.gen(function* () {
  const actor = yield* createEffectActor(uploadProgress);
  return yield* snapshots(actor).pipe(
    Stream.filter((s) => s.context !== undefined),
    Stream.map((s) => s.context),
    Stream.runLast,
    Effect.map(Option.getOrThrow)
  );
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 100
```

The demo stream is finite. Replace it with your upload SDK's progress stream; the actor exposes each new percentage as `snapshot.context`.

- Context is `undefined` until the first item arrives.
- Completion puts the actor in `done`, with no output; the last context remains readable.
- Stream failure puts the actor in `error`.
- Read changing values with `snapshots`, `waitFor` or a UI selector.

## `createEffectEventStreamLogic`

Each stream item enters the stream actor's mailbox. Its transition forwards the item to the parent as an event and publishes a snapshot notification from the stream actor.

Use `createEffectEventStreamLogic` when stream items should trigger transitions. This deployment feed first reports a healthy rollout, then a health failure that requests rollback:

<!-- example from examples/effect-workflows/src/event-stream.ts -->

```ts
import { Effect, Stream } from 'effect';
import {
  createEffectActor,
  createEffectEventStreamLogic,
  join,
  setupEffect
} from '@xstate/effect';

// A deployment feed drives the workflow, rather than only displaying data.
const deploymentEvents = createEffectEventStreamLogic(
  Stream.make({ type: 'HEALTHY' }, { type: 'UNHEALTHY' })
);

const rolloutMachine = setupEffect({
  actors: { deploymentEvents }
}).createMachine({
  output: () => 'rollback requested',
  initial: 'monitoring',
  states: {
    monitoring: {
      invoke: { src: 'deploymentEvents' },
      initial: 'checking',
      states: {
        checking: { on: { HEALTHY: { target: 'healthy' } } },
        healthy: {}
      },
      on: { UNHEALTHY: { target: 'rollingBack' } }
    },
    rollingBack: { type: 'final' }
  }
});

const program = Effect.gen(function* () {
  const actor = yield* createEffectActor(rolloutMachine);
  return yield* join(actor);
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 'rollback requested'
```

The stream belongs to `monitoring`, so it stays active when that state moves from `checking` to `healthy`. Leaving `monitoring` stops the stream actor and interrupts its consumption.

- Each item is relayed to the parent machine as an event.
- Completion puts the stream actor in `done`, with no output.
- Stream failure puts it in `error`; handle it with the invocation's `onError`.

Use this pattern for WebSocket messages, job updates or subscription feeds that change a workflow's state.

## Tracing

The three functions run inside spans named `createEffectLogic`, `createEffectStreamLogic` and `createEffectEventStreamLogic`. See [tracing](testing-and-errors.md#tracing).
