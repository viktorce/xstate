---
'@xstate/effect': patch
---

Document how a state timer can drop an invoked Effect task's result, and how racing the deadline inside the task with `Effect.raceFirst` or `Effect.timeoutOption` removes the competing timer. Other transitions out of the invoking state, or stopping the actor, can still discard an item the task took.
