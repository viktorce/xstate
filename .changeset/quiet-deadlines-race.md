---
'xstate': patch
---

Document when an `after`, state `timeout` or invoke `timeout` racing an invoked actor can lose a result the actor had not reported, such as a queue item it already took, and which remedy fits each case. The guide now includes a reserve, then commit example for queues that redeliver uncommitted items.
