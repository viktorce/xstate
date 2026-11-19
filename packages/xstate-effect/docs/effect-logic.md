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

A state timer and an invoked task are separate fibers that report to the machine through one mailbox, and the machine handles events in the order they arrive. An `after`, a state `timeout` or an invoke `timeout` is one event and the task's result is another. A task that finished before the timer fired wins.

The loss happens when the timer's event is already in the mailbox and the task then takes an item from a `Queue`. That can happen even if the item was offered before the timer fired, because Effect resumes a waiting `Queue.take` on a later scheduler step. The machine handles the timer on a later step of its own fiber, and the task keeps running until then. How long that takes depends on the Effect scheduler. If the timer's transition exits the invoking state, the machine interrupts the task and the item it took is lost. A targetless timer handler leaves the task running. Nothing reports the drop. A late result finds no transition, and it is not a dead letter.

When the result must not be lost to a timer, race the deadline inside the task. `Effect.raceFirst` interrupts the loser, and an interrupted `Queue.take` leaves the item in the queue:

<!-- example from examples/effect-workflows/src/deadline.ts -->

```ts
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

// One Effect owns both outcomes. When the deadline wins, the take is
// interrupted before it removes anything from the queue.
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
```

- `onDone` receives either outcome, so the race removes the competing timer. Another transition out of `waiting`, or stopping the actor, can still interrupt the task after it took an item and discard that item.
- The consuming step must be safe to interrupt. `Queue.take` is, in effect 4.0.6. Other sources may not be.
- Nothing asynchronous may follow the consuming step inside the raced branch, only synchronous mapping such as `Effect.map`. Avoid a branch shaped like `Queue.take(q).pipe(Effect.flatMap(save))`, which can lose the deadline race after it took the item.
- `Effect.timeoutOption` works the same way and returns `Option.none()` at the deadline.
- The deadline restarts in full when the actor is restored, because a running Effect task starts again from the beginning. An `after` timer keeps its original deadline. See [persisting and restoring](actors.md#persisting-and-restoring).
- Keep `after` and `timeout` for work that is safe to repeat or abandon.

The core guide covers the same race for `createAsyncLogic` in [racing an invocation against a timer](../../../docs/invoke.md#racing-an-invocation-against-a-timer).

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
