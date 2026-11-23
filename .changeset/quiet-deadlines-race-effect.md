---
'@xstate/effect': patch
---

Document how a state timer can drop an invoked Effect task's result, and how racing the deadline inside the task with `Effect.raceFirst` or `Effect.timeoutOption` avoids that when the raced branch holds only the take and one `Effect.map`. Any further Effect after the take can let the deadline win after the item was taken. Another transition out of the invoking state, or stopping the actor, can still discard an item the task took.
