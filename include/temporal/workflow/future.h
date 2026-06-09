#pragma once

#include <coroutine>
#include <memory>
#include <type_traits>
#include <utility>

#include <temporal/common/errors.h>
#include <temporal/converter/data_converter.h>
#include <temporal/internal/workflow_outbound.h>

namespace temporal::workflow {

// Handle to the eventual result of a workflow operation (activity, timer).
// `Get()` blocks the workflow until the result is available — matching the Go
// SDK's `future.Get(ctx, &out)` ergonomics — by parking the workflow task when
// the value is not yet known and resuming on a later task once it is.
template <class T>
class Future {
 public:
  Future(std::shared_ptr<internal::FutureState> state, const DataConverter* converter,
         internal::WorkflowOutbound* env)
      : state_(std::move(state)), converter_(converter), env_(env) {}

  [[nodiscard]] bool IsReady() const { return state_->ready; }

  // Request cancellation of the underlying operation (timers today). After this,
  // Get() unblocks immediately (the operation is treated as cancelled) rather
  // than waiting for it to complete. Deterministic: it emits a cancel command
  // the workflow must reproduce on replay.
  void Cancel() { env_->Cancel(state_); }

  T Get() {
    env_->Block(state_);  // throws internal::WorkflowBlocked if not yet ready
    if (state_->failed) {
      throw ActivityError(state_->failure_type, state_->failure_message);
    }
    if constexpr (std::is_void_v<T>) {
      return;
    } else {
      return converter_->template FromPayload<T>(state_->result.at(0));
    }
  }

  // Enables `co_await fut` in a workflow_task coroutine (see <temporal/workflow/coro.h>).
  // Equivalent to Get(): it blocks the workflow through the same stackful engine and
  // yields the typed result, so command order + replay are unchanged.
  auto operator co_await() {
    struct Awaiter {
      Future fut;
      bool await_ready() { return false; }
      bool await_suspend(std::coroutine_handle<>) { return false; }  // resume now; block in resume
      T await_resume() { return fut.Get(); }
    };
    return Awaiter{std::move(*this)};
  }

 private:
  std::shared_ptr<internal::FutureState> state_;
  const DataConverter* converter_;
  internal::WorkflowOutbound* env_;
};

}  // namespace temporal::workflow
