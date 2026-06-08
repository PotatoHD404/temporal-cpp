---
title: Errors, retries & timeouts
description: How to signal failures from activities, configure retry policies and timeouts, and handle errors in workflow code.
---

# Errors, retries & timeouts

This page covers how the SDK propagates failures between activities and workflows,
how you control retry behavior and timeouts, and what happens when a workflow
itself fails or encounters a non-determinism violation.

All error types live in `<temporal/common/errors.h>`.

## Error hierarchy

```
std::runtime_error
  └── temporal::TemporalError
        ├── temporal::ApplicationError     — thrown by your activity/workflow code
        ├── temporal::ActivityError        — received in the workflow when an activity fails
        ├── temporal::WorkflowFailedError  — received by a client when a workflow ends badly
        ├── temporal::DataConverterError   — payload encode/decode failure
        └── temporal::RpcError             — transport / gRPC failure to the Temporal service
```

## Throwing failures from an activity

Inside an activity, throw `temporal::ApplicationError` to signal a named,
typed failure. The constructor takes a human-readable message, an optional
**type string**, and an optional **non-retryable flag**:

```cpp
// Retryable by default (the server will try again per the retry policy):
throw temporal::ApplicationError("downstream service unavailable", "ServiceUnavailable");

// Non-retryable: the server will not schedule another attempt.
throw temporal::ApplicationError("card declined — do not retry", "CardDeclined",
                                 /*non_retryable=*/true);

// Message only (type is empty string, retryable):
throw temporal::ApplicationError("unexpected nil response");
```

The type string is a free-form identifier you choose. It is used in two ways:

1. **`RetryPolicy::non_retryable_error_types`** — if the type of a thrown
   `ApplicationError` appears in this list, the activity is treated as
   non-retryable regardless of the `non_retryable` constructor flag.
2. **Catch sites in the workflow** — you can inspect `ActivityError::type()` to
   distinguish failure kinds.

You can also throw `ApplicationError` from a workflow or an update validator; see
[Update validators](/advanced#update-validators) for that usage.

## Catching activity failures in a workflow

When a `Future<R>::Get()` call resolves to a failed activity (one that exhausted
all retry attempts or threw a non-retryable error), it throws
`temporal::ActivityError`. The `type()` accessor returns the error type string
from the original `ApplicationError`.

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);
opts.retry_policy.maximum_attempts = 3;
opts.retry_policy_set = true;

try {
  std::string receipt =
      ctx.ExecuteActivity<std::string>(opts, "ChargeCard", order_id, amount).Get();
  // activity succeeded
} catch (const temporal::ActivityError& e) {
  ctx.GetLogger().Error("ChargeCard failed", {{"type", e.type()}, {"what", e.what()}});

  if (e.type() == "CardDeclined") {
    // non-retryable, handle gracefully
    return "payment-declined";
  }
  throw;  // re-throw other failures to fail the workflow
}
```

:::note
Only `temporal::ActivityError` is thrown by `Future::Get()` for activity
failures. You do not need to catch `ApplicationError` in workflow code — that
exception type is for the *activity side* to throw, not the workflow side to
receive.
:::

## Configuring retries with `RetryPolicy`

`RetryPolicy` is a field on `ActivityOptions`. To apply a non-default policy you
**must** also set `retry_policy_set = true`; without it the field is ignored and
the server applies its built-in defaults.

```cpp
struct RetryPolicy {
  std::chrono::milliseconds initial_interval{1000};  // first retry delay (default 1 s)
  double backoff_coefficient{2.0};                   // multiplier per retry
  std::chrono::milliseconds maximum_interval{0};     // 0 => 100 × initial_interval
  int maximum_attempts{0};                           // 0 => unlimited
  std::vector<std::string> non_retryable_error_types;
};
```

### Typical patterns

**Limit attempts and tune backoff:**

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);

opts.retry_policy.maximum_attempts      = 5;
opts.retry_policy.initial_interval      = std::chrono::milliseconds(500);
opts.retry_policy.backoff_coefficient   = 1.5;
opts.retry_policy.maximum_interval      = std::chrono::seconds(30);
opts.retry_policy_set = true;  // required — applies the policy above
```

**Mark specific error types as non-retryable by name:**

```cpp
opts.retry_policy.non_retryable_error_types = {"CardDeclined", "InvalidInput"};
opts.retry_policy_set = true;
```

If the activity throws `ApplicationError("...", "CardDeclined")`, the server
stops retrying immediately — even if `maximum_attempts` has not been reached
and the `non_retryable` constructor flag is `false`.

**Fail on the first attempt (no retries):**

This is the pattern used in the integration tests for the `FailWorkflow` case:

```cpp
opts.retry_policy.maximum_attempts = 1;
opts.retry_policy_set = true;
```

:::warning
Forgetting `retry_policy_set = true` is a common mistake. Until you set it, all
`RetryPolicy` fields are ignored and the server falls back to its own defaults
(unlimited retries with exponential backoff). Always set the flag together with
the policy fields.
:::

## Activity timeouts

`ActivityOptions` exposes four timeout fields:

```cpp
struct ActivityOptions {
  std::chrono::milliseconds schedule_to_close_timeout{0}; // total budget from schedule to finish
  std::chrono::milliseconds schedule_to_start_timeout{0}; // max time waiting in the task queue
  std::chrono::milliseconds start_to_close_timeout{0};    // per-attempt execution time (effectively required)
  std::chrono::milliseconds heartbeat_timeout{0};         // max gap between RecordHeartbeat calls
  // ...
};
```

`start_to_close_timeout` is the one you almost always set. It bounds how long a
single execution attempt may run before the server considers the activity failed
and schedules a retry (or a final failure if retries are exhausted). The comment
in the header marks it *effectively required* — the server will reject a schedule
command that carries no timeout at all.

```cpp
temporal::ActivityOptions opts;
opts.start_to_close_timeout = std::chrono::seconds(30);  // each attempt has 30 s
```

### Heartbeat timeout

For long-running activities that call `ctx.RecordHeartbeat()`, set
`heartbeat_timeout` to a value shorter than the longest gap between heartbeat
calls. If the activity fails to heartbeat within the window, the server treats
the activity as timed out and schedules a retry:

```cpp
opts.start_to_close_timeout  = std::chrono::minutes(10);
opts.heartbeat_timeout        = std::chrono::seconds(15);  // must heartbeat every 15 s
opts.retry_policy.maximum_attempts = 3;
opts.retry_policy_set = true;
```

Inside the activity, heartbeat regularly and check `IsCancelled()` so the
activity can stop cooperatively when the server requests it:

```cpp
std::string ProcessLargeFile(temporal::activity::Context& ctx, std::string path) {
  for (int chunk = 0; chunk < total_chunks; ++chunk) {
    process(path, chunk);
    ctx.RecordHeartbeat(chunk);      // resets the heartbeat clock; returns cancel flag
    if (ctx.IsCancelled()) {
      return "cancelled";            // stop promptly when requested
    }
  }
  return "done";
}
```

### What a timeout looks like to the workflow

A timed-out activity follows the same retry path as any other failure. After the
last retry is exhausted (or if `maximum_attempts` was 1), `Future::Get()` throws
`temporal::ActivityError`. The `type()` string in that case will be
`"TimeoutType_SCHEDULE_TO_CLOSE"`, `"TimeoutType_START_TO_CLOSE"`, or
`"TimeoutType_HEARTBEAT"` — you can match on it if you need to distinguish a
timeout from an application-level failure:

```cpp
try {
  result = ctx.ExecuteActivity<std::string>(opts, "ProcessLargeFile", path).Get();
} catch (const temporal::ActivityError& e) {
  if (e.type().find("Timeout") != std::string::npos) {
    ctx.GetLogger().Warn("activity timed out", {{"type", e.type()}});
  }
  throw;
}
```

## Workflow failure

A workflow function that **throws an unhandled exception** (anything other than
the SDK's internal control-flow signals for `ContinueAsNew`, timers, etc.) fails
the workflow execution. The Temporal server marks the execution `FAILED` and
`WorkflowFailedError` is thrown on the client side when `WorkflowHandle::Get()`
is called.

On the **client**, catching workflow failure looks like:

```cpp
try {
  std::string result = handle.Get<std::string>();
} catch (const temporal::WorkflowFailedError& e) {
  std::cerr << "Workflow failed: " << e.what() << '\n';
}
```

:::note
If you deliberately want to fail a workflow from within its code, throw
`temporal::ApplicationError`. Throwing any other unhandled exception also fails
the workflow, but `ApplicationError` is the idiomatic, typed way to signal an
intentional terminal failure.
:::

## Non-determinism and `WorkflowPanicPolicy`

A different class of failure occurs when the workflow engine replays history and
the workflow's commands do not match what was recorded. This is a
**non-determinism violation** — it means the workflow code changed in an
incompatible way while executions were in flight.

The SDK responds per `WorkflowPanicPolicy`, configured on the worker:

```cpp
temporal::WorkerOptions wopts;
wopts.panic_policy = temporal::WorkflowPanicPolicy::BlockWorkflow;  // default
temporal::worker::Worker worker(client, "my-task-queue", wopts);
```

- **`BlockWorkflow`** (default) — fails only the current **workflow task**. The
  server retries the task, so deploying a corrected worker recovers the
  workflow without data loss. Mirrors the Go/Java SDKs' `BlockWorkflow` behavior.
- **`FailWorkflow`** — fails the entire **workflow execution**. Terminal; use
  only when a stuck, unrecoverable workflow is worse than a failed one.

See [Advanced capabilities: Non-determinism detection](/advanced#non-determinism-detection)
for the full treatment, including `GetVersion` for safe code evolution and the
[Replay testing](/advanced#replay-testing) pattern to catch violations in CI
before they reach production.

## What is not implemented

The following capabilities appear in the official Temporal SDKs but are **not
yet available** in this SDK (see the [parity matrix](/parity)):

- **Custom failure converters** — you cannot plug in a custom encoder/decoder
  for failure payloads. The SDK uses its built-in failure representation.
- **Proto / ProtoJSON converters** — only the JSON converter stack is available,
  so error details carried as Protobuf messages are not supported.
- **Async (manual) completion** — an activity cannot complete via a token from
  outside its goroutine/thread.
- **Activity-side cancellation** (`RequestCancelActivityTask` without a
  `Future::Cancel()` from the workflow) — cancellation is observable only through
  the heartbeat path when the workflow explicitly calls `act.Cancel()`.

If you need one of these, check the [parity matrix](/parity) for current status
before building on top of it.
