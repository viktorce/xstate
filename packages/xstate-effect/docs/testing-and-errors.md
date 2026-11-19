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

- Timers use Effect's `Clock`, including delayed sends.
- `waitFor` and `join` fail if the actor stops before reaching the expected result.
- Add `waitFor`'s `{ timeout }` to bound a wait. With `TestClock`, advance the clock to trigger that timeout too.
- Use `inspect` for execution traces and `deadLetters` for delivery failures.

XState's [path generation](../../../docs/model-based-testing.md) works on the machine itself. Execute generated paths against an actor from `createEffectActor`, or use `testPaths()` from `@xstate/test`.

### Start the timer before advancing the clock

`TestClock.adjust` does not wait for an actor that is still on its way to a delayed transition. It wakes the sleeps that are due and waits for the work each woken fiber schedules, so a sleep that fiber starts next is covered too. It does not follow an actor through invoked work that yields or does I/O, and it does not wait out a long run of queued events or a slow step. In those cases the clock moves before the actor enters the state that owns the `after`. The timer then counts from the new time, and the test times out in a later `waitFor`.

The queued-event and slow-step cases come from the actor loop, which [yields under continuous traffic so timers and other Effects can run](actors.md). It decides when to yield partly by elapsed real time, so a test that sends several events and advances the clock with no wait can pass locally and fail on a slower CI machine.

Wait for the state that starts the timer, then advance the clock:

<!-- example from examples/effect-workflows/src/clock-armed.ts -->

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
  // The yield stands in for real I/O. It is why the test below must wait for
  // `backoff` before it advances the clock.
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
```

- The 30-second example above needs no wait. Its timer belongs to the initial state, so the actor starts it during startup, before `adjust` wakes anything.
- `sendAndAwait` acknowledges the event you sent. It does not wait for an invoked actor's `onDone` or `onError`, so use `waitFor` when the timer's state follows one.
- To check that nothing happens before a deadline, wait for the state that starts the timer, advance to just short of the delay, then assert. Without the wait, the assertion can pass because the timer has not started.
- When an invoked actor runs between two timers, advance the clock once per timer and wait for the second timer's state in between.
- When the invoked Effect itself sleeps, retries on a schedule or times out, it waits on the test clock too. A `waitFor` on the later state never returns until the clock moves. Advance the clock for the invoked work first, then wait for the timer's state, then advance the clock for the timer.
- The same limit applies inside the invoked Effect. If it yields or does I/O before it sleeps, no actor state marks the moment the sleep starts. Have the test's version of that Effect signal the test just before it sleeps, for example by completing a `Deferred`, and await the signal before advancing the clock.
- `waitFor` succeeds at once when the actor already matches. If the actor can already be in the timer's state when the test sends an event that leaves and re-enters it, acknowledge the event with `sendAndAwait` first, then wait for the state.

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
