---
'xstate': patch
---

Document that an `after`, state `timeout` or invoke `timeout` racing an invoked actor can drop a result the actor had not reported yet, such as a queue item it already took. Giving the actor its own deadline removes the competing timer. Other transitions out of the invoking state can still drop an unreported result.

```ts
// The logic owns the deadline, so the machine has no timer racing it.
const takeSignal = createAsyncLogic({
  timeout: 4_000,
  run: ({ signal }) => signals.take(signal)
});
```
