---
title: Advanced capabilities
description: Determinism detection, SideEffect, GetVersion, update validators, cancellation, replay testing, and memo.
---

# Advanced capabilities

Beyond the core programming model, the SDK implements the determinism-critical and
production-leaning features below. Each is exercised by the test suite against a
real `temporal server start-dev`.

## Non-determinism detection

A workflow must be deterministic: replayed against its recorded history, it has to
emit exactly the same orchestration commands, in the same order. The engine
records the ordered stream of commands a workflow produces and matches it against
the command-generating events in history (modeled on the Go SDK's
`matchReplayWithHistory`). History is authoritative; the workflow may only emit
*additional* trailing commands (genuine forward progress).

A mismatch is surfaced per `WorkflowPanicPolicy`, set on the worker:

```cpp
temporal::WorkerOptions opts;
opts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // default
temporal::worker::Worker worker(client, "my-task-queue", opts);
```

- **`BlockWorkflow`** (default) — fail the workflow *task*. The server retries it,
  so deploying a corrected worker recovers the workflow without data loss.
- **`FailWorkflow`** — fail the workflow *execution* outright. Terminal.

The check runs only on a full-history replay (the resident sticky coroutine is the
source of truth and is never re-validated). See [Replay testing](#replay-testing)
to catch a non-deterministic change *before* you deploy it.

## SideEffect

`SideEffect` captures the result of a non-deterministic operation exactly once.
The first time it runs, your function executes and its result is recorded to
history; on every replay the recorded value is returned **without** running the
function again.

```cpp
int id = ctx.SideEffect<int>([] { return generate_random_id(); });
```

Use it for ids, randomness, or reading a clock — never for anything with
externally-visible effects (those belong in an activity).

## GetVersion (versioning / patching)

`GetVersion` lets you change workflow code while old executions are still running.
It records the chosen version the first time it runs and returns the recorded
version on replay, so both old and new histories stay deterministic.

```cpp
int v = ctx.GetVersion("greeting-change", temporal::workflow::kDefaultVersion, 1);
if (v == temporal::workflow::kDefaultVersion) {
  // original behavior, for executions that started before this change
  greet_v0(ctx);
} else {
  // new behavior
  greet_v1(ctx);
}
```

`kDefaultVersion` (-1) is returned when replaying history recorded *before* the
`GetVersion` call existed. Once every pre-change execution has drained, you can
drop the old branch and raise `min_supported`.

## Update validators

An update handler can take an optional **read-only validator** that runs *before*
the update is accepted. If the validator throws, the update is rejected and the
handler never runs — and because a rejection is **not written to history**,
workflow state is untouched.

```cpp
ctx.SetUpdateHandler(
    "deposit",
    [&](int amount) { balance += amount; return balance; },  // handler
    [](int amount) {                                          // validator
      if (amount <= 0) {
        throw temporal::ApplicationError("amount must be positive", "InvalidDeposit");
      }
    });
```

On the client, a rejected update surfaces as a thrown exception:

```cpp
handle.Update<int>("deposit", -5);  // throws — validator rejected it
```

Validators must be read-only (no activities, timers, or state mutation), exactly
like query handlers.

## Cancelling operations

Operation futures expose `Cancel()`. For a **timer**, cancelling emits a
`CancelTimer` command and resolves the future immediately, so an awaiter unblocks
at once instead of waiting the timer out:

```cpp
auto timer = ctx.NewTimer(std::chrono::minutes(5));
// ... something else happened ...
timer.Cancel();   // the workflow won't wait the full 5 minutes
timer.Get();      // returns right away
```

Cancellation is deterministic — the workflow reproduces the `CancelTimer` command
on replay.

To **react** to a workflow-level cancellation (the "clean up and stop" pattern),
wait on `ctx.AwaitCancellation()` as a `Selector` case, racing it against your
work:

```cpp
auto work = ctx.NewTimer(std::chrono::minutes(5));
std::string out;
temporal::workflow::Selector sel(ctx);
sel.AddFuture(work, [&] { out = "done"; });
sel.AddFuture(ctx.AwaitCancellation(), [&] {
  work.Cancel();
  out = "cancelled";
});
sel.Select();
```

When the workflow is cancelled, `AwaitCancellation` completes, the selector takes
that branch, and the workflow cancels its timer and finishes promptly. Activity
and child-workflow cancellation are not wired yet — the activity *side* is exposed
(`activity::Context::IsCancelled` observes a server cancel via heartbeat), but the
workflow→activity `RequestCancelActivityTask` trigger is the remaining piece.

## Replay testing

Replay a recorded history against your current workflow code — **without a
server** — to catch a non-deterministic change before it reaches production. This
is the single most valuable workflow unit test you can write.

```cpp
// Export a real history (e.g. from a workflow you just ran, or
// `temporal workflow show -o json`):
std::string history_json = handle.FetchHistoryJson();

// Replay it against the registered workflow; throws on non-determinism.
temporal::worker::Worker replayer(client, "replay");
replayer.RegisterWorkflow("MyWorkflow", MyWorkflow);
replayer.ReplayWorkflowHistory(history_json);  // throws if MyWorkflow diverged
```

Keep a few representative histories as test fixtures and replay them in CI; any
incompatible edit to a workflow (a reordered activity, a removed timer) fails the
test instead of a running workflow in production.

## Memo & Describe

Attach non-indexed metadata to a workflow at start, and read a point-in-time
snapshot back:

```cpp
temporal::StartWorkflowOptions o;
o.task_queue = "my-task-queue";
o.memo["owner"] = dc->ToPayload(std::string("alice"));
auto handle = client.StartWorkflow(o, "MyWorkflow");

temporal::client::WorkflowDescription d = handle.Describe();
// d.status == "RUNNING", d.run_id, d.memo["owner"] -> "alice"
```

`Describe()` returns the workflow id, run id, status (e.g. `RUNNING`, `COMPLETED`),
and the memo. Memo is not indexed for search; typed/indexed search attributes are
not implemented yet.
