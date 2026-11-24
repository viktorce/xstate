---
title: "XState Effect: Testing and errors"
description: Test deadlines, recover from failures and supervise workflow actors.
---

Test the workflow with its real machine and provide local Effect services. Use `waitFor` for state assertions and `join` for final output.

## Acknowledging an event

Import `sendAndAwait` from `@xstate/effect` when you need acknowledgment that one particular send was processed. The persistence example also uses it to checkpoint after each request.

It returns that mailbox item's snapshot after its transition is published and its immediate effects have run, including its child actors' handling of events it sent them, if those effects complete. Ignored events succeed with an unchanged snapshot. An event whose action fails also succeeds; the failure is processed afterwards, through `onError` or an error snapshot, unless the actor has stopped by then. Concurrent sends of the same object get independent receipts. An event that completes or errors the actor returns its terminal snapshot; use `join` for its outcome.

Invalid and externally sent internal events fail with `EventRejectedError`. Stop, scope closure or completion before processing a queued send fails it with `ActorStoppedError`. Interrupting the waiter detaches the receipt without retracting the event. A reentrant listener or observer stop rejects the currently processed receipt only when the step leaves the actor active.

This helper does not await invoked tasks, background actions, child replies such as an invoked child's `onDone`, future timers or global quiescence. Keep `waitFor` or a domain-specific completion signal for those conditions.

Existing `@xstate/test` SUT sessions accept async sends. Set their `send` function to an adapter that runs `sendAndAwait` and resolves to void, and `read` to the actor's snapshot projection. The package test suite exercises this through the real `testPaths` API.

XState's [path generation](../../../docs/model-based-testing.md) works on the machine itself. Execute generated paths against an actor from `createEffectActor`, or use `testPaths()` from `@xstate/test`.

## TestClock

A release approval expires after 30 seconds. `TestClock` drives the same `after` transition without waiting for real time:

<!-- example from examples/effect-workflows/src/clock.ts -->

```ts
import { Effect } from 'effect';
import { TestClock } from 'effect/testing';
import { createEffectActor, waitFor } from '@xstate/effect';
import { createMachine } from 'xstate';

const reviewMachine = createMachine({
  initial: 'pending',
  states: {
    pending: {
      after: { 30000: { target: 'expired' } },
      on: { APPROVE: { target: 'approved' } }
    },
    approved: { type: 'final' },
    expired: { type: 'final' }
  }
});

const test = Effect.gen(function* () {
  const actor = yield* createEffectActor(reviewMachine);
  yield* TestClock.adjust('30 seconds');
  const snapshot = yield* waitFor(actor, (s) => s.matches('expired'));
  return snapshot.value;
});

export const result = await Effect.runPromise(
  test.pipe(Effect.scoped, Effect.provide(TestClock.layer()))
);
console.log(result); // 'expired', with no 30-second wait
```

This test advances the clock with no wait. Its timer belongs to the initial state, so the actor starts it during startup. When a later state starts the timer, [wait for that state first](#wait-for-the-timer-to-start-before-advancing-the-clock).

- Timers use Effect's `Clock`, including delayed sends.
- `waitFor` and `join` fail if the actor stops before reaching the expected result.
- Add `waitFor`'s `{ timeout }` to bound a wait. With `TestClock`, advance the clock to trigger that timeout too.
- Use `inspect` for execution traces and `deadLetters` for delivery failures.
- The examples and rules in this section use `TestClock.layer()` with Effect's default scheduler.

### Wait for the timer to start before advancing the clock

`TestClock.adjust` fires the timers that come due during the advance. It can return before an actor enters the state that starts an `after` timer. That timer then counts from the new time, and that call to `adjust` does not fire it. A `waitFor` for the timer's transition hangs until the test runner stops the test. A `{ timeout }` on that `waitFor` does not help, because the timeout runs on the test clock too. An assertion that nothing happened can also pass for the wrong reason, because the timer has not started.

Wait for the state that starts the timer, then advance the clock:

<!-- example from examples/effect-workflows/src/clock-after-invoke.ts -->

```ts
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
```

The clock can move first in cases like these:

- The actor reaches the timer's state through invoked work or a background action that yields, awaits a Promise or does I/O. Any of these can let the clock advance before the actor starts the timer. If that work sleeps on the test clock, [wait for its sleep to start and advance the clock for it](#wait-for-a-sleep-to-start-before-advancing-the-clock) first.
- A long run of queued events or a slow step comes before the event that starts the timer. The actor loop yields under continuous traffic, as the [actors guide](actors.md) describes, and it decides when to yield partly by elapsed real time. A test that sends several events and advances the clock with no wait can pass locally and fail on a slower CI machine.

Details for applying the wait:

- `sendAndAwait` is enough when the event you send enters the timer's state. It does not wait for an invoked actor's `onDone` or `onError`, so use `waitFor` when one of those transitions enters the timer's state.
- When an invoked actor runs between two timers, advance the clock for the first timer, wait for the state that starts the second, then advance for the second. One `adjust` across both can finish before the second timer starts. If the invoked Effect sleeps on the test clock, [wait for its sleep to start and advance for it](#wait-for-a-sleep-to-start-before-advancing-the-clock) before that wait.
- A delayed `raise` or `sendTo` starts its timer when its action runs, and the actor can stay in the same state. When an event the test sends runs the action, acknowledge that event with `sendAndAwait`, then advance the clock. When a later transition runs it, such as an invoke's `onDone`, wait for a state or context change that the transition makes.
- `waitFor` succeeds at once when the actor already matches, so it cannot see the actor leave a state and come back. When an event takes the actor out of the state you wait for and a later transition brings it back, acknowledge the event with `sendAndAwait` first, then wait for the state. When the event re-enters the state in the same step, as a transition with `reenter: true` does to reset its timer, you do not need `waitFor` after `sendAndAwait`.

### Wait for a sleep to start before advancing the clock

Invoked Effects and background actions can sleep on the test clock themselves. An advance that runs before such a sleep starts does not count toward it. The sleep still needs its full duration, and a `waitFor` on the state after the work hangs. Wait for each sleep to start, then advance the clock for it.

- A sleep that comes before any yield or I/O starts in the step that starts the work. Wait for the state that invokes the Effect or runs the action. When the event you send starts the work without a state change, as a restart of an invoke with `reenter: true` does, acknowledge that event with `sendAndAwait`.
- A sleep that follows a yield or I/O has no actor state to mark its start, so the test needs a signal from inside the work. Create an unbounded `Queue` in the test, offer to it just before each sleep and take from it before each advance.

This test makes the offer from its implementation of a service that the Effect calls right before it sleeps:

<!-- example from examples/effect-workflows/src/clock-sleep-signal.ts -->

```ts
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
```

When the work calls no service before it sleeps, make the offer from a replacement Effect or action that you give `createEffectActor` through `machine.provide({ actors })` or `machine.provide({ actions })`.

## Retry a task

Use Effect's retry combinators inside `createEffectLogic` for transient failures within one task. This local publisher succeeds on its third attempt:

<!-- example from examples/effect-workflows/src/retry.ts -->

```ts
import { Effect, Schedule } from 'effect';
import { createEffectActor, createEffectLogic, join } from '@xstate/effect';

let attempts = 0;
const publish = createEffectLogic(
  Effect.suspend(() => {
    attempts++;
    return attempts < 3
      ? Effect.fail(new Error('Publisher temporarily unavailable'))
      : Effect.succeed('published');
  }).pipe(
    Effect.retry({ schedule: Schedule.exponential('10 millis'), times: 3 })
  )
);

const program = Effect.gen(function* () {
  const actor = yield* createEffectActor(publish);
  return yield* join(actor);
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 'published', on attempt 3
```

Use a machine state when retry requires a person or an event. The [quick start](quick-start.md) accepts `RETRY` only after deployment fails.

## Supervise an actor

Retry a scoped unit when a failure should create a fresh actor:

<!-- example from examples/effect-workflows/src/supervision.ts -->

```ts
import { Effect, Schedule } from 'effect';
import { createEffectActor, createEffectLogic, join } from '@xstate/effect';

let attempts = 0;
const worker = createEffectLogic(
  Effect.suspend(() => {
    attempts++;
    return attempts < 3
      ? Effect.fail(new Error('Worker disconnected'))
      : Effect.succeed('complete');
  })
);

const attempt = Effect.gen(function* () {
  const actor = yield* createEffectActor(worker);
  return yield* join(actor);
});

// Each failed attempt closes its scope before a new actor starts.
const supervised = Effect.scoped(attempt).pipe(
  Effect.retry({ schedule: Schedule.exponential('10 millis'), times: 3 })
);

export const result = await Effect.runPromise(supervised);
console.log(result); // 'complete', from the third actor
```

Each attempt stops its actor and releases its resources before the next attempt starts.

<details>
<summary>Choosing failures to retry</summary>

`join` reports the actor's failure, so the retry policy sees that error. A machine's error type is `unknown`; narrow it if only some failures should be retried. Keep `Effect.scoped` inside the retry, so failed actors are released after each attempt.

Retrying can repeat external operations. Use stable operation IDs or idempotent APIs when repetition could create duplicate work.

</details>

## Handle typed failures

A `createEffectLogic` actor preserves its Effect's error type. Handle a domain failure with `Effect.catchTag`:

<!-- example from examples/effect-workflows/src/errors.ts -->

```ts
import { Data, Effect } from 'effect';
import { createEffectActor, createEffectLogic, join } from '@xstate/effect';

class PublishFailed extends Data.TaggedError('PublishFailed')<{
  readonly reason: string;
}> {}

const publish = createEffectLogic(
  Effect.fail(new PublishFailed({ reason: 'Release needs approval' }))
);

const program = Effect.gen(function* () {
  const actor = yield* createEffectActor(publish);
  return yield* join(actor).pipe(
    Effect.catchTag('PublishFailed', (error) => Effect.succeed(error.reason))
  );
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 'Release needs approval'
```

For machine workflows, expected outcomes such as declined or expired can be final states with output. `join(machineActor)` has an `unknown` error channel for unexpected machine failures.

## Error reference

| Error | Raised by |
| --- | --- |
| `ActorStoppedError` | `waitFor` when the actor stops or errors before matching; `join` when it stops without output. |
| `EffectFailure<E>` | Task, stream or hosted action failure, defect or self-interruption. Its `cause` preserves the full Effect Cause. |
| `Cause.TimeoutError` | `waitFor` with a timeout, or Effect's timeout combinators. |

- `ActorStoppedError` includes `actorId` and `snapshot`.
- `snapshot.error` and invoked child `onError` events carry `EffectFailure<E>` for Effect logic. `ErrorFrom` reflects that wrapper.
- `join` and result atoms unwrap genuine `EffectFailure` values and preserve the original Cause. Plain XState actor errors remain ordinary typed failures.
- `EffectInterruptedError` is deprecated. Inspect `Cause.hasInterruptsOnly(error.cause)` instead.

### Migrating error handlers

Effect child error handlers now receive `EffectFailure<E>`. Checks such as `event.error instanceof DigestMismatch` no longer match the original domain error. Replace these checks and direct property access with `Cause.findErrorOption(causeFromError(event.error))`, then inspect the returned failure.

Import `causeFromError` from `@xstate/effect`. It returns an `EffectFailure`'s original Cause and wraps ordinary errors with `Cause.fail`. This also supports handlers receiving mixed error types. Its return type is `Cause.Cause<UnwrapEffectFailure<E>>`; an `unknown` input keeps the failure type `unknown`.

`Cause.findErrorOption` returns `None` for a Cause containing only defects or interruption. Handle that case separately. `Cause.squash` may return a defect, so use `Cause.findErrorOption` when matching typed failures.

<!-- example from examples/effect-workflows/src/failure-cause.ts -->

```ts
import { Cause, Effect, Option } from 'effect';
import {
  causeFromError,
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor
} from '@xstate/effect';

const request = createEffectLogic(Effect.fail({ code: 'NOT_FOUND' as const }));
const machine = setupEffect({ actors: { request } }).createMachine({
  context: { code: '' },
  initial: 'pending',
  states: {
    pending: {
      invoke: {
        src: 'request',
        onError: ({ event }) => {
          const failure = Cause.findErrorOption(causeFromError(event.error));
          return {
            target: 'failed',
            context: {
              code: Option.isSome(failure) ? failure.value.code : 'UNEXPECTED'
            }
          };
        }
      }
    },
    failed: {}
  }
});

const program = Effect.gen(function* () {
  const actor = yield* createEffectActor(machine);
  return (yield* waitFor(actor, (snapshot) => snapshot.matches('failed')))
    .context.code;
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 'NOT_FOUND'
```

Use `Effect.exit(join(actor))` when defects and interruption must be inspected alongside typed failures. Stopping the actor still makes `join` fail with `ActorStoppedError`.

<details>
<summary>Observe root actor errors</summary>

A root Effect actor's error is a value. Read it with `join`, `waitFor`, `subscribe` or the `result` atom. An unobserved errored actor is silent, like a failed forked fiber.

</details>

## Persistence

`actor.getPersistedSnapshot()` records the actor's state. It does not record progress inside a running Effect: an Effect task or stream that was running when the snapshot was taken starts again from the beginning after a restore. See [persisting and restoring](actors.md#persisting-and-restoring).

## Tracing

Hosted Effects run inside spans:

| Span | Work |
| --- | --- |
| `createEffectLogic` | A task actor's Effect. |
| `createEffectStreamLogic` | A stream of snapshot values. |
| `createEffectEventStreamLogic` | A stream of parent events. |
| `action.<name>` | A declared Effect action. |

Spans carry `xstate.actor.id` and `xstate.actor.address`. The address is the actor's `/`-joined path from the root, identifying its place in the actor tree.

Provide a `Tracer` to record and export spans.
