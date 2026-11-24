---
"@xstate/effect": patch
---

Document when to advance `TestClock` in actor tests. `TestClock.adjust` can return before an actor enters the state that starts an `after` timer, so wait for that state first.

```ts
actor.send({ type: 'CONNECT' });
yield* waitFor(actor, (s) => s.matches('backoff'));
yield* TestClock.adjust('3 seconds');
```
