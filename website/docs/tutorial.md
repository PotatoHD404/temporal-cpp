---
title: "Tutorial — your first workflow"
description: Build a real order-processing workflow end-to-end with the Temporal C++ SDK.
---

# Tutorial — your first workflow

This tutorial walks you through building a realistic, multi-step workflow from scratch: an **order
pipeline** that validates an order, charges a card, and sends a confirmation — then extends it with a
signal so an operator can approve orders before they're charged.

You'll write real C++ that compiles and runs against a local Temporal server. If you haven't built
the SDK yet, start with [Getting started](./getting-started) first.

## What you'll build

- Two activities: `ValidateOrder` and `ChargeCard`
- A workflow: `ProcessOrder`, which calls them in sequence and returns a receipt
- A worker process that registers and runs everything
- A client that starts the workflow and prints the result
- An approval signal that pauses the workflow until an operator says "go"

The finished code fits in a single `main.cpp` and uses only the headers included via
`<temporal/temporal.h>`.

---

## Part 1 — activities

Activities are the workhorses of Temporal: they run in real time, may do I/O, and are retried
automatically on failure. An activity is a plain C++ function whose first parameter is
`temporal::activity::Context&`.

```cpp
#include <temporal/temporal.h>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace std::chrono_literals;

// ── activities ────────────────────────────────────────────────────────────────

// Validate an order. Returns the validated total or throws on bad input.
double ValidateOrder(temporal::activity::Context& ctx,
                     std::string order_id, double amount) {
  ctx.GetInfo();  // available if you need activity_id, attempt, etc.
  if (amount <= 0) {
    // Non-retryable: the workflow receives this as a fatal ActivityError.
    throw temporal::ApplicationError(
        "order amount must be positive", "InvalidOrder", /*non_retryable=*/true);
  }
  // Real code would hit a database, an internal API, etc.
  return amount;
}

// Charge the card. Returns a receipt string.
std::string ChargeCard(temporal::activity::Context& ctx,
                       std::string order_id, double amount) {
  // Simulate a payment provider call. Real code talks to Stripe, etc.
  return "receipt-" + order_id + "-" + std::to_string(static_cast<int>(amount * 100));
}
```

:::note
`temporal::ApplicationError` carries a string **type** (`"InvalidOrder"`) that retry policies and
catch sites can discriminate. Pass `non_retryable = true` to tell the server not to retry — here, a
bad amount will never succeed no matter how many times we try.
:::

---

## Part 2 — the workflow

A workflow is also a plain function, but its first parameter is `temporal::workflow::Context&` and
its body must be **deterministic**: no wall-clock reads, no random numbers, no direct I/O. All of
that lives in activities.

`ctx.ExecuteActivity<R>(opts, "ActivityName", args...)` schedules the activity and returns a
`Future<R>`. Calling `.Get()` on the future parks the workflow until the activity completes
(or fails).

```cpp
// ── workflow ──────────────────────────────────────────────────────────────────

std::string ProcessOrder(temporal::workflow::Context& ctx,
                         std::string order_id, double amount) {
  // Options apply to every ExecuteActivity call; you can reuse or customise per call.
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;        // each activity has up to 30 s to finish
  opts.retry_policy.maximum_attempts = 3;   // retry transient failures up to 3 times
  opts.retry_policy_set = true;

  // Step 1 — validate.
  // Future<double>::Get() either returns the validated amount or throws ActivityError.
  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Step 2 — charge.
  const std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();

  ctx.GetLogger().Info("order processed", {{"order_id", order_id}, {"receipt", receipt}});
  return receipt;
}
```

:::tip
Activities scheduled **before** the first `.Get()` run in parallel. Here they're sequential because
each step depends on the previous one. For fan-out patterns (e.g. charging multiple line items at
once) schedule all futures first, then call `.Get()` on each — see
[Activities & timers](./workflows/overview) for a parallel example.
:::

---

## Part 3 — worker and client

A `Worker` polls a task queue and dispatches tasks to your registered functions. A `Client` starts
workflows and interacts with them. The client and worker share the same task queue name — that's how
Temporal routes tasks.

```cpp
// ── worker + client ───────────────────────────────────────────────────────────

int main() {
  // Connect to the local dev server. Default target is localhost:7233, namespace "default".
  auto client = temporal::client::Client::Connect(
      {.target = "localhost:7233", .ns = "default"});

  // Build a worker on the "orders" task queue.
  temporal::worker::Worker worker(client, "orders");

  // Register every workflow and activity type the worker should handle.
  // The string name is the "type" used in StartWorkflow / ExecuteActivity.
  worker.RegisterWorkflow("ProcessOrder", ProcessOrder);
  worker.RegisterActivity("ValidateOrder", ValidateOrder);
  worker.RegisterActivity("ChargeCard",    ChargeCard);

  // Start() spawns poller threads and returns immediately.
  // Run() would block until SIGINT — useful for a standalone worker process.
  worker.Start();

  // Start a workflow execution. The id is optional; omitting it generates a UUID.
  temporal::StartWorkflowOptions wf_opts;
  wf_opts.id         = "order-001";
  wf_opts.task_queue = "orders";

  auto handle = client.StartWorkflow(wf_opts, "ProcessOrder",
                                     std::string("order-001"), 49.99);

  // Result<R>() long-polls until the workflow closes, then decodes the return value.
  // It throws WorkflowFailedError if the workflow failed, timed out, or was terminated.
  try {
    const std::string receipt = handle.Result<std::string>();
    std::cout << "Receipt: " << receipt << "\n";
  } catch (const temporal::WorkflowFailedError& e) {
    std::cerr << "Workflow failed: " << e.what() << "\n";
    worker.Stop();
    return 1;
  }

  worker.Stop();
  return 0;
}
```

### Running it

```bash
# Terminal 1 — start the local dev server
temporal server start-dev

# Terminal 2 — build and run
cmake --preset default && cmake --build build -j
./build/examples/order_pipeline/order_pipeline
# Receipt: receipt-order-001-4999
```

Open the Temporal Web UI at **http://localhost:8233** and find `order-001` in the workflow list.
Click through to see the full event history: `WorkflowExecutionStarted`, two `ActivityTaskScheduled`
/ `ActivityTaskCompleted` pairs, and `WorkflowExecutionCompleted`.

---

## Part 4 — adding an approval signal

Your order pipeline charges immediately, but some orders need a human to approve them first. Signals
let you pause a workflow and wait for an external event.

A signal channel is typed: `ctx.GetSignalChannel<T>("signal-name")` returns a `ReceiveChannel<T>`.
Calling `.Receive()` parks the workflow deterministically — just like `.Get()` on a future — until a
signal with that name arrives.

```cpp
// ── revised workflow with approval gate ───────────────────────────────────────

std::string ProcessOrderWithApproval(temporal::workflow::Context& ctx,
                                     std::string order_id, double amount) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;
  opts.retry_policy.maximum_attempts = 3;
  opts.retry_policy_set = true;

  // Step 1 — validate (same as before).
  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Step 2 — wait for an operator to approve.
  // The workflow parks here; the server durably holds the signal until we receive it.
  auto approval_ch = ctx.GetSignalChannel<std::string>("approve");
  const std::string decision = approval_ch.Receive();  // "yes" or "no"

  if (decision != "yes") {
    // Return a non-receipt to indicate rejection — or throw ApplicationError.
    return "rejected";
  }

  // Step 3 — charge (only after approval).
  const std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();

  return receipt;
}
```

### Sending the signal from the client

The signal payload is encoded via the [data converter](./data-conversion) before being sent. Use
`DataConverter::Default()` to get the same JSON converter the SDK uses by default:

```cpp
// Register the new workflow type alongside the old one, or replace it:
worker.RegisterWorkflow("ProcessOrderWithApproval", ProcessOrderWithApproval);

temporal::StartWorkflowOptions wf_opts;
wf_opts.id         = "order-002";
wf_opts.task_queue = "orders";

auto handle = client.StartWorkflow(wf_opts, "ProcessOrderWithApproval",
                                   std::string("order-002"), 120.00);

// The workflow is now parked, waiting for the "approve" signal.
// In a real system a separate process (e.g. a review dashboard) sends this.
const auto dc = temporal::DataConverter::Default();
handle.Signal("approve", dc->ToPayloads(std::string("yes")));

const std::string receipt = handle.Result<std::string>();
std::cout << "Receipt: " << receipt << "\n";
```

:::tip
`Signal()` is fire-and-forget: it returns as soon as the RPC succeeds; the workflow may not have
consumed it yet. If you need confirmation that the signal has been **applied** to workflow state,
register a query handler and poll — see [Signals, queries & updates](./workflows/messaging).
:::

---

## Part 5 — a timer for the approval deadline

What if no one approves for 24 hours? You can race the signal against a timer using a `Selector`.
`Selector` picks the first future (or channel) that becomes ready and fires its callback.

```cpp
#include <temporal/workflow/selector.h>

std::string ProcessOrderWithDeadline(temporal::workflow::Context& ctx,
                                     std::string order_id, double amount) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;
  opts.retry_policy.maximum_attempts = 3;
  opts.retry_policy_set = true;

  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Race approval signal against a 24-hour deadline.
  auto approval_ch  = ctx.GetSignalChannel<std::string>("approve");
  auto deadline_fut = ctx.NewTimer(24h);  // durable: survives worker restarts

  std::string decision;
  bool timed_out = false;

  temporal::workflow::Selector sel(ctx);
  // AddFuture<T> callback receives the typed result when the future fires.
  sel.AddFuture<void>(deadline_fut, [&]() { timed_out = true; });
  // For a signal channel, use AddFuture on a Future built from the signal itself —
  // or simply call Receive() if you don't need racing. Here we need the race,
  // so we call Receive() inside a timer: if deadline fires first, Receive() never runs.
  // The simpler pattern: just check timed_out after Select().
  //
  // Because selector channel cases are not yet supported (see parity matrix),
  // we use a one-shot future approach: schedule the timer and check IsCancelled
  // as an escape hatch, OR use the two-future pattern below with a helper workflow.
  //
  // Simplest correct pattern with current selectors:
  sel.Select();  // blocks until the timer fires OR we get interrupted

  if (timed_out) {
    return "expired";
  }

  // If we're here the timer fired — in a real workflow you'd park on the signal
  // separately, or use Sleep + ReceiveAsync in a loop:
  std::string sig;
  if (approval_ch.ReceiveAsync(sig)) {
    if (sig != "yes") return "rejected";
    return ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();
  }
  return "expired";
}
```

:::note
The pattern above is a simplification. For a clean "race signal vs. timer" today, the most
straightforward approach is `ctx.Sleep(deadline)` followed by `ReceiveAsync` in a loop — or use a
regular `Receive()` and let the operator cancel the workflow to trigger the timeout path. Selector
**channel** cases (which would let you `AddChannel` directly) are not yet implemented; see the
[parity matrix](./parity) for the current status.
:::

The more idiomatic pattern that works today — `Sleep` + `ReceiveAsync`:

```cpp
std::string ProcessOrderWithDeadline(temporal::workflow::Context& ctx,
                                     std::string order_id, double amount) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = 30s;

  const double validated =
      ctx.ExecuteActivity<double>(opts, "ValidateOrder", order_id, amount).Get();

  // Poll the signal channel in small increments up to 24 hours.
  // ctx.Sleep() is durable — the workflow survives restarts between sleeps.
  auto approval_ch = ctx.GetSignalChannel<std::string>("approve");
  constexpr auto kPollInterval = 5min;
  constexpr int  kMaxPolls     = (24 * 60) / 5;  // 288 polls = 24 hours

  for (int i = 0; i < kMaxPolls; ++i) {
    std::string decision;
    if (approval_ch.ReceiveAsync(decision)) {
      if (decision != "yes") return "rejected";
      return ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, validated).Get();
    }
    ctx.Sleep(kPollInterval);
  }
  return "expired";  // 24 hours passed with no signal
}
```

---

## Full picture

Here's how the pieces fit together:

```
Client                            Temporal Server               Worker
──────                            ───────────────               ──────
StartWorkflow("ProcessOrder") ──► route to "orders" queue ──► ProcessOrder(ctx, ...)
                                                                  │
                                                                  ├─ ExecuteActivity("ValidateOrder")
                                                                  │    ValidateOrder(ctx, ...) ◄── activity task
                                                                  │
                                                                  ├─ GetSignalChannel("approve").Receive()
                                                                  │    (workflow parked in sticky cache)
                                                                  │
handle.Signal("approve", "yes") ─►  deliver signal              │
                                                                  │    resume → charge step
                                                                  └─ ExecuteActivity("ChargeCard")
                                                                       ChargeCard(ctx, ...) ◄── activity task

handle.Result<string>() ◄─────── workflow closed (receipt)
```

Key invariants:
- The worker registers each function **once** by name. The name string must match exactly what you
  pass to `StartWorkflow` / `ExecuteActivity`.
- Workflows are deterministic; activities are not. If you need a random id, a current timestamp, or
  any external data, get it from an activity.
- The sticky cache keeps running workflows in memory, so signals and updates apply to live state
  without a full replay. When the cache is cold (new worker, restart) the engine replays history to
  reconstruct state — which is why workflow code must be deterministic.

---

## What to read next

| Topic | Doc |
|---|---|
| Activities in depth (timeouts, retries, heartbeating) | [Activities & timers](./workflows/overview) |
| Signals, queries, and updates | [Signals, queries & updates](./workflows/messaging) |
| Client options, `GetHandle`, `Describe` | [Client & worker](./client-and-worker) |
| Parity with the official SDKs (what's implemented and what isn't) | [Capabilities & parity](./parity) |
| JSON encoding, custom converters | [Data conversion](./data-conversion) |
| Replay testing without a live server | [Testing](./testing) |
