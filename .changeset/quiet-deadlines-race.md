---
'xstate': patch
'@xstate/effect': patch
---

Document that an `after`, state `timeout` or invoke `timeout` racing an invoked actor can drop the actor's result, and show how to keep it by letting the actor own the deadline.

```ts
// The logic owns the deadline, so it cannot take a signal and report a timeout.
const takeSignal = createAsyncLogic({
  timeout: 4_000,
  run: ({ signal }) => signals.take(signal)
});
```

With `@xstate/effect`, race the deadline inside the task with `Effect.raceFirst` or `Effect.timeoutOption`.
