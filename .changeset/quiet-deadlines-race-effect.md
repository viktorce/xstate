---
'@xstate/effect': patch
---

Document how a state timer can drop an invoked Effect task's result, and how racing the deadline inside the task with `Effect.raceFirst` or `Effect.timeoutOption` keeps the item it took on Effect 4.0.2 or later. The guide says which branch shape keeps the item and which steps after the take can lose it.
