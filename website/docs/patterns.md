---
title: Patterns & recipes
description: Self-contained C++ recipes for the most common Temporal workflow patterns, grounded in the SDK's real API.
---

# Patterns & recipes

Each recipe below is copy-pasteable and compiles against the SDK's real API.
The integration test suite (`tests/integration/integration_test.cpp`) is the
authoritative source; the patterns here are adapted directly from it.

For fundamentals see [Activities & timers](workflows/overview.md),
[Signals, queries & updates](workflows/messaging.md),
[Selectors, child workflows & more](workflows/composition.md), and
[Advanced capabilities](advanced.md).

:::note
Only capabilities marked ✅ in the [parity matrix](parity.md) are covered here.
Patterns that require unimplemented features (local activities, automatic
cancellation propagation, MutableSideEffect, etc.) are intentionally omitted.
:::

---

## 1. Sequential activity chain

Run a sequence of activities one after another, passing the result of each into
the next.

```cpp
#include <temporal/temporal.h>

// Activity: increments an integer.
int AddOneActivity(temporal::activity::Context&, int n) { return n + 1; }

// Workflow: chains the activity N times sequentially.
int ChainWorkflow(temporal::workflow::Context& ctx, int steps) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);

  int value = 0;
  for (int i = 0; i < steps; ++i) {
    // Schedule, then immediately await: the next iteration only starts
    // after the previous activity completes.
    value = ctx.ExecuteActivity<int>(opts, "AddOne", value).Get();
  }
  return value;  // equals `steps` after N "+1" calls
}
```

Register and start:

```cpp
worker.RegisterWorkflow("ChainWorkflow", ChainWorkflow);
worker.RegisterActivity("AddOne", AddOneActivity);

temporal::StartWorkflowOptions o;
o.task_queue = "my-queue";
auto handle = client.StartWorkflow(o, "ChainWorkflow", 5);
int result = handle.Result<int>();  // 5
```

---

## 2. Parallel fan-out / fan-in

Schedule several activities *before* awaiting any of them so they run
concurrently. Collect all results once they are complete.

```cpp
int ParallelWorkflow(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);

  // Fan-out: all three activities are scheduled in the same workflow task.
  auto f1 = ctx.ExecuteActivity<int>(opts, "AddOne", base);
  auto f2 = ctx.ExecuteActivity<int>(opts, "AddOne", base + 10);
  auto f3 = ctx.ExecuteActivity<int>(opts, "AddOne", base + 100);

  // Fan-in: each Get() parks until that specific future is ready.
  return f1.Get() + f2.Get() + f3.Get();
}
```

:::tip
Scheduling N activities before calling `Get()` is the correct way to achieve
concurrency — the SDK records all N schedule commands in the same workflow task,
and the server can dispatch them in parallel.
:::

---

## 3. Activity-vs-timeout race with `Selector`

Use a `Selector` to proceed with whichever completes first: the activity result
or a deadline timer. This is the canonical "call with a deadline" pattern.

```cpp
#include <temporal/workflow/selector.h>

std::string SlowActivity(temporal::activity::Context&, int sleep_ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
  return "done";
}

std::string WithTimeoutWorkflow(temporal::workflow::Context& ctx,
                                int activity_ms, int timeout_ms) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(30);

  // Schedule both; neither blocks yet.
  auto activity = ctx.ExecuteActivity<std::string>(opts, "SlowActivity", activity_ms);
  auto deadline = ctx.NewTimer(std::chrono::milliseconds(timeout_ms));

  std::string result;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(activity, [&](std::string r) {
    result = "activity: " + r;
  });
  sel.AddFuture(deadline, [&]() {
    result = "timed out";
  });
  sel.Select();  // blocks until the first ready case fires its handler
  return result;
}
```

Both futures are scheduled before `Select()` is called, so the race is genuine.
The losing future keeps running in the background; if you want to cancel it,
call `activity.Cancel()` or `deadline.Cancel()` inside the winning handler.

---

## 4. Long-running "entity" workflow

Accept signals to accumulate state, answer queries from observers, and finish
cleanly on a sentinel signal. This is the entity/actor pattern in Temporal.

```cpp
int EntityWorkflow(temporal::workflow::Context& ctx) {
  int balance = 0;

  // Expose current state to read-only callers — no activities or state mutation
  // inside the query handler.
  ctx.SetQueryHandler("balance", [&]() -> int { return balance; });

  // Accept "deposit" updates that mutate state and return the new balance.
  ctx.SetUpdateHandler("deposit", [&](int amount) -> int {
    balance += amount;
    return balance;
  });

  // Process "credit" signals (fire-and-forget) for cases where the caller
  // does not need a synchronous acknowledgement.
  auto credits = ctx.GetSignalChannel<int>("credit");

  // A "close" signal terminates the loop.
  auto close = ctx.GetSignalChannel<std::string>("close");

  while (true) {
    // Non-blocking drain of any queued credit signals before checking close.
    int c = 0;
    while (credits.ReceiveAsync(c)) {
      balance += c;
    }

    std::string cmd;
    if (close.ReceiveAsync(cmd)) {
      break;
    }

    // Nothing ready — park until the next signal or update arrives.
    // Use a very short timer as a yield point so the scheduler can process
    // buffered signals that arrived before this task.
    ctx.Sleep(std::chrono::milliseconds(10));
  }

  return balance;
}
```

From a client:

```cpp
auto dc = temporal::DataConverter::Default();

// Mutating call — blocks until the update is applied.
int new_balance = handle.Update<int>("deposit", 100);

// Signal — fire-and-forget.
handle.Signal("credit", dc->ToPayloads(50));

// Read-only query against live state.
int snapshot = handle.Query<int>("balance");

// Finish the workflow.
handle.Signal("close", dc->ToPayloads(std::string("done")));
int final_balance = handle.Result<int>();
```

:::note
Signals are buffered and ordered. A `ReceiveAsync` that returns `false` means
the channel has no queued messages at that moment, not that the signal will
never arrive. For a simpler blocking variant, replace the loop body with
`balance += credits.Receive()` and handle the sentinel value explicitly.
:::

---

## 5. Continue-as-new loop

For a workflow that must run indefinitely, `ContinueAsNew` atomically completes
the current run and starts a fresh one with a bounded history. Use it whenever
you expect history to grow without an end.

```cpp
// Counts down via continue-as-new, then returns 0 on the final run.
int CountdownWorkflow(temporal::workflow::Context& ctx, int n) {
  if (n <= 0) {
    return 0;
  }
  // Terminal: does not return. Starts a new run of the same workflow type
  // with the decremented argument.
  ctx.ContinueAsNew("CountdownWorkflow", n - 1);
}
```

`ContinueAsNew` never returns — mark anything after it `[[unreachable]]` or
simply return without a value (the compiler may warn; the throw is internal).
The client's `Result<R>()` transparently follows the run chain to the final
execution:

```cpp
auto handle = client.StartWorkflow(o, "CountdownWorkflow", 3);
int result = handle.Result<int>();  // 0, after runs 3 -> 2 -> 1 -> 0
```

:::tip
A common pattern for a processing loop is to call `ContinueAsNew` after a
fixed number of iterations (e.g., every 1 000 items), passing the carry-over
state as arguments to the new run.
:::

---

## 6. Capture non-determinism with `SideEffect` and version branches with `GetVersion`

### `SideEffect` — record a random/external value exactly once

`SideEffect` runs its function on the first execution and records the result to
history. On every replay the recorded value is returned without re-running the
function. Use it for random IDs, wall-clock reads, or any value that must be
stable across replays but cannot come from an activity.

```cpp
#include <random>

std::string CreateOrderWorkflow(temporal::workflow::Context& ctx) {
  // The random ID is generated once and fixed for the lifetime of this run.
  int order_id = ctx.SideEffect<int>([] {
    std::mt19937 rng(std::random_device{}());
    return std::uniform_int_distribution<int>(1, 1'000'000)(rng);
  });

  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(30);
  return ctx.ExecuteActivity<std::string>(
      opts, "ReserveInventory", order_id).Get();
}
```

:::note
`SideEffect`'s function must return a value — `R` cannot be `void`. Use an
activity for anything with externally-visible side effects.
:::

### `GetVersion` — safe code changes for in-flight workflows

`GetVersion` lets you deploy a code change while old executions are still
running. It records the chosen version on first execution and returns it on
every replay, keeping both branches deterministic.

```cpp
std::string ProcessOrderWorkflow(temporal::workflow::Context& ctx,
                                 std::string order_id) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(30);

  // `kDefaultVersion` (-1) is returned for histories recorded *before* this
  // call was added. Once all pre-change runs have drained you can remove the
  // v0 branch and raise min_supported to 1.
  int v = ctx.GetVersion(
      "add-fraud-check",
      temporal::workflow::kDefaultVersion,  // min_supported
      1);                                   // max_supported

  if (v == temporal::workflow::kDefaultVersion) {
    // Original path: no fraud check.
    return ctx.ExecuteActivity<std::string>(
        opts, "ChargeCard", order_id).Get();
  } else {
    // New path: fraud check first.
    ctx.ExecuteActivity<void>(opts, "FraudCheck", order_id).Get();
    return ctx.ExecuteActivity<std::string>(
        opts, "ChargeCard", order_id).Get();
  }
}
```

Multiple independent changes each get their own `change_id` string. Version
markers are recorded in history in the order `GetVersion` is first called, so
add them in the same place in the code every time.

---

## 7. React to workflow cancellation

Race active work against `ctx.AwaitCancellation()` in a `Selector`. When the
workflow is cancelled the cancellation future fires first; cancel the timer (or
activity), then return promptly.

### Cancel a timer on workflow cancellation

```cpp
std::string CancelAwareWorkflow(temporal::workflow::Context& ctx) {
  // A long-running timer simulating background work.
  auto timer = ctx.NewTimer(std::chrono::minutes(60));
  auto cancelled = ctx.AwaitCancellation();

  std::string result;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture(timer, [&]() {
    result = "timer-fired";
  });
  sel.AddFuture(cancelled, [&]() {
    timer.Cancel();   // resolves the timer immediately; no 60-minute wait
    result = "cancelled";
  });
  sel.Select();
  return result;
}
```

### Cancel an activity on workflow cancellation

```cpp
// Activity: heartbeats so the server's cancel reaches it promptly.
std::string LongRunningActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ctx.RecordHeartbeat(i);
    if (ctx.IsCancelled()) {
      return "cancelled";  // exit cleanly on cancel
    }
  }
  return "finished";
}

std::string ActivityCancelWorkflow(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(60);
  opts.heartbeat_timeout = std::chrono::seconds(5);

  auto act = ctx.ExecuteActivity<std::string>(opts, "LongRunningActivity", 0);
  auto cancelled = ctx.AwaitCancellation();

  bool cancel_requested = false;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [&](std::string) { /* completed naturally */ });
  sel.AddFuture(cancelled, [&]() {
    act.Cancel();           // sends RequestCancelActivityTask to the server
    cancel_requested = true;
  });
  sel.Select();

  // If we requested cancel, wait for the activity's final result.
  // The activity returns "cancelled" once it observes IsCancelled() in its
  // heartbeat loop.
  return cancel_requested ? act.Get() : "finished";
}
```

From the client:

```cpp
auto handle = client.StartWorkflow(o, "ActivityCancelWorkflow");
// ... let it run a few seconds ...
handle.Cancel();
std::string result = handle.Result<std::string>();  // "cancelled"
```

:::note
Cancellation propagation to activities requires the activity to heartbeat
(`RecordHeartbeat`) and poll `IsCancelled()`. An activity that does not heartbeat
will never observe the cancel request via `ctx.IsCancelled()`. Set a
`heartbeat_timeout` on `ActivityOptions` to ensure the server times out an
unresponsive activity promptly if the worker disappears. See
[Capabilities & parity](parity.md) for the current cancellation scope.
:::

---

## 8. Update with validation

Reject invalid input *before* the update is accepted and written to history.
The validator runs first; throwing prevents the handler from executing and
leaves workflow state untouched.

```cpp
int AccountWorkflow(temporal::workflow::Context& ctx) {
  int balance = 0;

  ctx.SetUpdateHandler(
    "deposit",
    // Handler: mutates state and returns the new balance.
    [&](int amount) -> int {
      balance += amount;
      return balance;
    },
    // Validator (read-only): throwing rejects the update; nothing is recorded.
    [](int amount) {
      if (amount <= 0) {
        throw temporal::ApplicationError(
            "deposit amount must be positive", "InvalidDeposit",
            /*non_retryable=*/true);
      }
    });

  ctx.GetSignalChannel<std::string>("close").Receive();
  return balance;
}
```

```cpp
int b1 = handle.Update<int>("deposit", 100);   // 100 — accepted
int b2 = handle.Update<int>("deposit",  50);   // 150 — accepted
handle.Update<int>("deposit", -5);             // throws — validator rejected
int b3 = handle.Update<int>("deposit",  20);   // 170 — state unchanged by rejection
```

---

## 9. Replay-safe history testing

Export a real workflow's history and replay it against your code without
contacting a server. Catches non-deterministic changes before they reach
production.

```cpp
// 1. Run the workflow and export its history.
std::string history_json = handle.FetchHistoryJson();

// 2. Replay the same code: no throw expected.
{
  temporal::worker::Worker replayer(client, "replay-queue");
  replayer.RegisterWorkflow("MyWorkflow", MyWorkflowV1);
  replayer.RegisterActivity("MyActivity", MyActivity);
  replayer.ReplayWorkflowHistory(history_json);  // clean
}

// 3. Replay changed code: should throw on non-determinism.
{
  temporal::worker::Worker replayer(client, "replay-queue");
  replayer.RegisterWorkflow("MyWorkflow", MyWorkflowV2);  // incompatible edit
  replayer.RegisterActivity("MyActivity", MyActivity);
  // throws std::exception — command sequence diverged from recorded history
  replayer.ReplayWorkflowHistory(history_json);
}
```

:::tip
Store a handful of exported histories as test fixtures and replay them in CI.
Any reorder, addition, or removal of activities/timers fails the fixture test
rather than silently breaking an in-flight execution in production.
See [Testing](testing.md) for the full replay-testing guide.
:::
