---
"@xstate/effect": patch
---

Document when to advance `TestClock` in actor tests. `TestClock.adjust` does not wait for an actor that is still on its way to a delayed transition, so wait for the state that starts the timer first.

```ts
actor.send({ type: 'CONNECT' });
yield* waitFor(actor, (s) => s.matches('backoff'));
yield* TestClock.adjust('3 seconds');
```
