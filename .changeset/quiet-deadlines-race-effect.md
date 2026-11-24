---
'@xstate/effect': patch
---

Document how a state timer can lose an item that an invoked Effect task took, and how to race the deadline inside the task instead. On Effect 4.0.2 or later, the documented branch of `Queue.take` and one `Effect.map`, raced with `Effect.raceFirst`, either wins with the item or leaves it in the queue.
