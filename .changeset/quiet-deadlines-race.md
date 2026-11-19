---
'xstate': patch
---

Document when an `after`, state `timeout` or invoke `timeout` racing an invoked actor can drop a result the actor had not reported yet, such as a queue item it already took. On `createActor` with the default clock and real timers, a child that follows the documented rules keeps its result against these timers. Giving the actor its own deadline removes the competing timer, including under `createEffectActor`. Any other transition out of the invoking state can still drop an unreported result.

```ts
// The logic owns the deadline, so the machine has no timer racing it.
const takeMessage = createAsyncLogic({
  timeout: 4_000,
  run: ({ signal }) => messages.take(signal)
});
```
