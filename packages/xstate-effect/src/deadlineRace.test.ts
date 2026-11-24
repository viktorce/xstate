import { Clock, Context, Duration, Effect, Option, Queue, Scope } from 'effect';
import { TestClock } from 'effect/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { AnyStateMachine } from 'xstate';
import {
  createRetryMachine as createAsyncRetryMachine,
  type MessageQueue
} from '../../core/test/fixtures/invoke-deadline/take-or-deadline.ts';
import {
  createEffectActor,
  createEffectLogic,
  setupEffect,
  waitFor,
  type EffectActor
} from './index.ts';

class Messages extends Context.Service<Messages, Queue.Queue<string>>()(
  '@test/Messages'
) {}

type Outcome = { kind: 'message'; message: string } | { kind: 'deadline' };

const toMessage = (message: string) => ({ kind: 'message' as const, message });

// The machine from examples/effect-workflows/src/deadline.ts and, for the
// pattern the docs warn about, an `after` timer. The example exports one
// machine, and the variants below need a factory, so this copies it.
function createRetryMachine(
  task: Effect.Effect<Outcome, never, Messages>,
  timer?: 'after'
) {
  const nextMessage = createEffectLogic(task);
  return setupEffect({ actors: { nextMessage } }).createMachine({
    context: { message: null as string | null },
    initial: 'waiting',
    output: ({ context }) => context,
    states: {
      waiting: {
        invoke: {
          src: 'nextMessage',
          onDone: ({ event }) =>
            event.output.kind === 'message'
              ? {
                  target: 'retryNow',
                  context: { message: event.output.message }
                }
              : { target: 'retryLater' }
        },
        after: timer ? { 4_000: { target: 'retryLater' } } : {}
      },
      retryNow: { type: 'final' },
      retryLater: { type: 'final' }
    }
  });
}

// A root machine that runs `child` as a nested machine and copies its result.
function createParentMachine(child: AnyStateMachine) {
  return setupEffect({ actors: { child } }).createMachine({
    context: { message: null as string | null },
    initial: 'waiting',
    states: {
      waiting: {
        invoke: {
          src: 'child',
          onDone: ({ event }) => ({
            target: event.output.message ? 'retryNow' : 'retryLater',
            context: { message: event.output.message }
          })
        }
      },
      retryNow: { type: 'final' },
      retryLater: { type: 'final' }
    }
  });
}

const takeMessage = (messages: Queue.Queue<string>) =>
  Queue.take(messages).pipe(Effect.map(toMessage));

const raceFirst = (
  branch = takeMessage,
  deadline: Duration.Input = '4 seconds'
) =>
  createRetryMachine(
    Messages.use((messages) =>
      Effect.raceFirst(
        branch(messages),
        Effect.sleep(deadline).pipe(Effect.as({ kind: 'deadline' as const }))
      )
    )
  );

const timeoutOption = createRetryMachine(
  Messages.use((messages) =>
    Queue.take(messages).pipe(
      Effect.timeoutOption('4 seconds'),
      Effect.map(
        Option.match({
          onNone: () => ({ kind: 'deadline' as const }),
          onSome: toMessage
        })
      )
    )
  )
);

const afterTimer = createRetryMachine(Messages.use(takeMessage), 'after');

// The example test covers the take-only Effect.raceFirst branch at a tie.
const machines = {
  'Effect.raceFirst': raceFirst(),
  'Effect.raceFirst and Effect.yieldNow after the take': raceFirst((messages) =>
    Queue.take(messages).pipe(
      Effect.tap(() => Effect.yieldNow),
      Effect.map(toMessage)
    )
  ),
  // The task saves the message for a second after the take.
  'Effect.raceFirst and an asynchronous save after the take': raceFirst(
    (messages) =>
      Queue.take(messages).pipe(
        Effect.flatMap((message) =>
          Effect.sleep('1 second').pipe(Effect.as(toMessage(message)))
        )
      )
  ),
  // The child's cleanup waits for a real millisecond.
  'Effect.raceFirst and a forked child with slow cleanup': raceFirst(
    (messages) =>
      Effect.forkChild(
        Effect.never.pipe(
          Effect.ensuring(
            Effect.promise(() => new Promise((done) => setTimeout(done, 1)))
          )
        ),
        { startImmediately: true }
      ).pipe(Effect.andThen(takeMessage(messages)))
  ),
  'Effect.raceFirst and 3,000 Effect.map calls after the take': raceFirst(
    (messages) =>
      Array.from({ length: 3_000 })
        .reduce(
          (take: Effect.Effect<string>) => take.pipe(Effect.map((m) => m)),
          Queue.take(messages)
        )
        .pipe(Effect.map(toMessage))
  ),
  'Effect.timeoutOption': timeoutOption,
  'an `after` timer': afterTimer,
  'an `after` timer in a nested machine': createParentMachine(afterTimer)
};

/**
 * A Clock whose sleeps end only when the test calls `wake`, so each test
 * decides the exact step in which a deadline or a timer ends.
 */
function createManualClock(clock: Clock.Clock) {
  const pending = new Set<{ millis: number; resume: () => void }>();
  const sleeping = (millis: number) =>
    [...pending].filter((sleep) => sleep.millis === millis);
  const manual: Clock.Clock = Object.create(clock);
  manual.sleep = (duration) =>
    Effect.callback<void>((resume) => {
      const sleep = {
        millis: Duration.toMillis(duration),
        resume: () => resume(Effect.void)
      };
      pending.add(sleep);
      return Effect.sync(() => void pending.delete(sleep));
    });
  return {
    clock: manual,
    /** Waits until a sleep of `millis` has started. */
    started: (millis: number) =>
      Effect.gen(function* () {
        for (let i = 0; i < 100 && !sleeping(millis).length; i++) {
          yield* Effect.yieldNow;
        }
        expect(sleeping(millis)).not.toHaveLength(0);
      }),
    /** Ends every pending sleep of `millis` in the calling step. */
    wake: (millis: number) => {
      for (const sleep of sleeping(millis)) {
        pending.delete(sleep);
        sleep.resume();
      }
    }
  };
}

function run<A, E>(
  effect: (
    manual: ReturnType<typeof createManualClock>
  ) => Effect.Effect<A, E, Messages | Scope.Scope>
) {
  return Effect.runPromise(
    Effect.gen(function* () {
      const messages = yield* Queue.unbounded<string>();
      const manual = createManualClock(yield* Clock.Clock);
      return yield* effect(manual).pipe(
        Effect.provideService(Messages, messages),
        Effect.provideService(Clock.Clock, manual.clock)
      );
    }).pipe(
      Effect.scoped,
      // TestClock fixes the time the actor reads for timer deadlines, and the
      // manual clock built on it replaces only `sleep`.
      Effect.provide(TestClock.layer())
    )
  );
}

const outcome = (
  actor: EffectActor<AnyStateMachine>,
  messages: Queue.Queue<string>
) =>
  Effect.gen(function* () {
    const { value, context } = yield* waitFor(
      actor,
      (s) => s.status === 'done'
    );
    return {
      value,
      message: context.message,
      size: yield* Queue.size(messages)
    };
  });

// `Queue.offerUnsafe` wakes a waiting take on a later scheduler step.
// `Queue.flushUnsafe` wakes it at once, so the take removes the item in the
// same step.
const offerAndTake = (messages: Queue.Queue<string>) => {
  Queue.offerUnsafe(messages, 'config changed');
  Queue.flushUnsafe(messages);
};

// Each schedule runs in one synchronous step, with nothing in between.
type Schedule = (messages: Queue.Queue<string>, wake: () => void) => void;
const schedules = {
  'offered to the waiting take before the timer ends': (messages, wake) => {
    offerAndTake(messages);
    wake();
  },
  'offered before the timer ends': (messages, wake) => {
    Queue.offerUnsafe(messages, 'config changed');
    wake();
  },
  'offered after the timer ends': (messages, wake) => {
    wake();
    offerAndTake(messages);
  }
} satisfies Record<string, Schedule>;

// Either winner is valid at a tie. Taking the message and then reporting the
// deadline is not, and neither is an `after` timer that drops it.
const outcomes = {
  'reports it': { value: 'retryNow', message: 'config changed', size: 0 },
  'keeps it queued': { value: 'retryLater', message: null, size: 1 },
  'loses it': { value: 'retryLater', message: null, size: 0 }
};

// The 3,000 Effect.map row covers the operation budget. Its threshold is an
// Effect internal, and 3,000 is far past it.
// A row that expects a loss is a deliberate alarm. The `after` rows depend on
// the root mailbox in createEffectActor.ts, and the others on Effect.
describe('a message that arrives at the instant the timer ends', () => {
  it.each([
    ['Effect.raceFirst', 'offered before the timer ends', 'keeps it queued'],
    [
      'Effect.raceFirst and Effect.yieldNow after the take',
      'offered to the waiting take before the timer ends',
      'loses it'
    ],
    [
      'Effect.raceFirst and an asynchronous save after the take',
      'offered to the waiting take before the timer ends',
      'loses it'
    ],
    [
      'Effect.raceFirst and a forked child with slow cleanup',
      'offered to the waiting take before the timer ends',
      'loses it'
    ],
    [
      'Effect.raceFirst and 3,000 Effect.map calls after the take',
      'offered to the waiting take before the timer ends',
      'loses it'
    ],
    [
      'Effect.timeoutOption',
      'offered to the waiting take before the timer ends',
      'reports it'
    ],
    [
      'Effect.timeoutOption',
      'offered before the timer ends',
      'keeps it queued'
    ],
    [
      'an `after` timer',
      'offered to the waiting take before the timer ends',
      'reports it'
    ],
    ['an `after` timer', 'offered before the timer ends', 'loses it'],
    ['an `after` timer', 'offered after the timer ends', 'loses it'],
    [
      'an `after` timer in a nested machine',
      'offered before the timer ends',
      'keeps it queued'
    ],
    [
      'an `after` timer in a nested machine',
      'offered after the timer ends',
      'keeps it queued'
    ]
  ] as const)(
    'with %s, when the message is %s, %s',
    async (timer, schedule, expected) => {
      const result = await run(({ started, wake }) =>
        Effect.gen(function* () {
          const messages = yield* Messages;
          const actor = yield* createEffectActor(machines[timer]);
          yield* started(4_000);
          // The 3,000 Effect.map branch yields once before its take waits.
          for (let i = 0; i < 3; i++) yield* Effect.yieldNow;
          yield* Effect.sync(() =>
            schedules[schedule](messages, () => wake(4_000))
          );
          return yield* outcome(actor, messages);
        })
      );
      expect(result).toEqual(outcomes[expected]);
    }
  );

  it('with Effect.uninterruptible around the take, reports no deadline until a message arrives, and loses it', async () => {
    const machine = raceFirst((messages) =>
      Queue.take(messages).pipe(Effect.uninterruptible, Effect.map(toMessage))
    );
    const result = await run(({ started, wake }) =>
      Effect.gen(function* () {
        const messages = yield* Messages;
        const actor = yield* createEffectActor(machine);
        yield* started(4_000);
        yield* Effect.sync(() => wake(4_000));
        for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
        const valueAfterDeadline = actor.getSnapshot().value;
        yield* Queue.offer(messages, 'config changed');
        return { valueAfterDeadline, ...(yield* outcome(actor, messages)) };
      })
    );
    expect(result).toEqual({
      valueAfterDeadline: 'waiting',
      ...outcomes['loses it']
    });
  });

  // The take frees capacity in a full bounded queue, which resumes the
  // blocked producer in the same step, and the producer ends the deadline.
  it('with Effect.raceFirst and a hand-written Clock that a producer wakes when the take frees capacity, loses it', async () => {
    const result = await run(({ started, wake }) =>
      Effect.gen(function* () {
        const messages = yield* Queue.bounded<string>(1);
        const actor = yield* createEffectActor(raceFirst()).pipe(
          Effect.provideService(Messages, messages)
        );
        yield* started(4_000);
        Queue.offerUnsafe(messages, 'config changed');
        yield* Effect.forkChild(
          Queue.offer(messages, 'next').pipe(
            Effect.andThen(Effect.sync(() => wake(4_000)))
          ),
          { startImmediately: true }
        );
        const { value, message } = yield* outcome(actor, messages);
        return { value, message, rest: yield* Queue.takeAll(messages) };
      })
    );
    expect(result).toEqual({
      value: 'retryLater',
      message: null,
      rest: ['next']
    });
  });
});

// Ties on the real clock, with the offer armed before or after the task arms
// its deadline. Real timers do not promise an order, so either winner is valid.
it('keeps the message at ties with Effect.raceFirst on the real clock', async () => {
  let deadlineArmed = false;
  const machine = createRetryMachine(
    Messages.use((messages) =>
      Effect.raceFirst(
        takeMessage(messages),
        // Sets the flag in the step that arms the sleep.
        Effect.sync(() => (deadlineArmed = true)).pipe(
          Effect.andThen(Effect.sleep(5)),
          Effect.as({ kind: 'deadline' as const })
        )
      )
    )
  );
  for (const messageFirst of [true, false, true, false, true, false]) {
    deadlineArmed = false;
    const result = await Effect.gen(function* () {
      const messages = yield* Queue.unbounded<string>();
      const offer = () =>
        new Promise<void>((done) =>
          setTimeout(() => {
            Queue.offerUnsafe(messages, 'config changed');
            done();
          }, 5)
        );
      const early = messageFirst ? offer() : undefined;
      const actor = yield* createEffectActor(machine).pipe(
        Effect.provideService(Messages, messages)
      );
      // An offer that fires first can end the race before the deadline starts.
      if (!messageFirst) {
        for (let i = 0; i < 100 && !deadlineArmed; i++) yield* Effect.yieldNow;
        expect(deadlineArmed).toBe(true);
      }
      yield* Effect.promise(() => early ?? offer());
      return yield* outcome(actor, messages);
    }).pipe(Effect.scoped, Effect.runPromise);
    expect([
      outcomes['reports it'],
      outcomes['keeps it queued']
    ]).toContainEqual(result);
  }
});

// The documented core deadline example under createEffectActor, with a take
// that is withdrawn when its abort signal aborts.
describe('the core deadline example under createEffectActor', () => {
  function createRetryActorLogic() {
    const items: string[] = [];
    let waiter: ((value: string) => void) | undefined;
    const queue: MessageQueue = {
      take: (signal) =>
        signal.aborted
          ? Promise.reject(signal.reason)
          : new Promise((resolvePromise, reject) => {
              waiter = resolvePromise;
              signal.addEventListener('abort', () => {
                waiter = undefined;
                reject(signal.reason);
              });
            })
    };
    const offer = (value: string) =>
      waiter ? waiter(value) : items.push(value);
    return { machine: createAsyncRetryMachine(queue), items, offer };
  }

  // Async logic times out on the global setTimeout, which vitest fakes here.
  // Effect schedules its steps with setImmediate, which stays real.
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // Waits until the child, started on the actor's fiber, arms its deadline.
  const armed = Effect.gen(function* () {
    for (let i = 0; i < 100 && vi.getTimerCount() === 0; i++) {
      yield* Effect.yieldNow;
    }
    expect(vi.getTimerCount()).toBeGreaterThan(0);
  });

  const settled = (actor: EffectActor<AnyStateMachine>) =>
    waitFor(actor, (s) => s.value !== 'waiting');

  it('keeps a message that arrives before the machine handles the timeout', async () => {
    const { machine, items, offer } = createRetryActorLogic();
    const result = await run(() =>
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        // The deadline is armed first, so it fires before the offer.
        yield* armed;
        let valueAtOffer: unknown;
        setTimeout(() => {
          valueAtOffer = actor.getSnapshot().value;
          offer('config changed');
        }, 4_000);
        // Both timers fire in one turn, so the message arrives after the
        // deadline aborted the take and before the machine handles it.
        vi.advanceTimersByTime(4_000);
        const { value } = yield* settled(actor);
        return { value, valueAtOffer };
      })
    );
    expect(result).toEqual({ value: 'retryLater', valueAtOffer: 'waiting' });
    expect(items).toEqual(['config changed']);
  });

  it('reports a message that the child sent while the root was still waiting', async () => {
    const { machine, offer } = createRetryActorLogic();
    const result = await run(() =>
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* armed;
        const valueAfterChildFinished = yield* Effect.promise(async () => {
          vi.advanceTimersByTime(3_999);
          offer('config changed');
          // The child finishes in microtasks, before Effect's next step.
          const [child] = Object.values(actor.getSnapshot().children);
          for (let i = 0; i < 100 && child.getSnapshot().status !== 'done'; i++)
            await Promise.resolve();
          expect(child.getSnapshot().status).toBe('done');
          vi.advanceTimersByTime(1);
          return actor.getSnapshot().value;
        });
        const { value, context } = yield* settled(actor);
        return { valueAfterChildFinished, value, message: context.message };
      })
    );
    expect(result).toEqual({
      valueAfterChildFinished: 'waiting',
      value: 'retryNow',
      message: 'config changed'
    });
  });

  // Delete this test and the guide sentence about TestClock when this timeout
  // moves to the Effect clock.
  it('runs an async logic timeout on the global timers, not on TestClock', async () => {
    const values = await Effect.runPromise(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(createRetryActorLogic().machine);
        yield* armed;
        yield* TestClock.adjust('5 seconds');
        for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
        const afterTestClock = actor.getSnapshot().value;
        vi.advanceTimersByTime(4_000);
        const snapshot = yield* settled(actor);
        return [afterTestClock, snapshot.value];
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer()))
    );
    expect(values).toEqual(['waiting', 'retryLater']);
  });
});
