---
'xstate': patch
---

Document when an `after`, state `timeout` or invoke `timeout` racing an invoked actor can drop a result the actor had not reported yet, such as a queue item it already took. On `createActor` with the default clock, a child that finished first wins. The loss there needs a child that keeps working after it consumed, a take that settles in a later task than it removes the item, or an exiting event sent in the task that settles the take. A child that follows the documented rules keeps its result. Giving the actor its own deadline leaves one actor to decide between the result and the deadline, which also holds under hosts that queue events, such as `createEffectActor`. Other transitions out of the invoking state can still drop an unreported result.

```ts
// The logic owns the deadline, so the machine has no timer racing it.
const takeMessage = createAsyncLogic({
  timeout: 4_000,
  run: ({ signal }) => messages.take(signal)
});
```
