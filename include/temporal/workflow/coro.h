#pragma once

#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>

namespace temporal::workflow {

// Opt-in C++20-coroutine authoring mode. A workflow may return workflow_task<R>
// and use `co_await` (on a Future) + `co_return` instead of `.Get()`:
//
//   temporal::workflow::workflow_task<std::string> Greet(Context& ctx, std::string n) {
//     std::string g = co_await ctx.ExecuteActivity<std::string>(opts, "Compose", n);
//     co_return g;
//   }
//
// It runs on the SAME stackful dispatcher as the plain-function form: `co_await`
// delegates blocking to the existing engine (Future::Get -> Block -> Yield), so the
// emitted command order and replay are IDENTICAL to the equivalent `.Get()`
// workflow — there is no separate scheduler and no determinism change. The task is
// eager (runs to completion when invoked); the worker reads the result from the
// promise. You can still call `.Get()` / channel `.Receive()` inside a coroutine
// workflow (they block the same way).
template <class R>
class workflow_task {
 public:
  struct promise_type {
    R value_{};
    std::exception_ptr error_;
    workflow_task get_return_object() {
      return workflow_task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_value(R v) { value_ = std::move(v); }
    void unhandled_exception() { error_ = std::current_exception(); }
  };
  using value_type = R;

  explicit workflow_task(std::coroutine_handle<promise_type> handle) : handle_(handle) {}
  workflow_task(workflow_task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  workflow_task& operator=(workflow_task&&) = delete;
  workflow_task(const workflow_task&) = delete;
  workflow_task& operator=(const workflow_task&) = delete;
  ~workflow_task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  // Result of the (already-run-to-completion) workflow; rethrows if it threw.
  R get() {
    if (handle_.promise().error_) {
      std::rethrow_exception(handle_.promise().error_);
    }
    return std::move(handle_.promise().value_);
  }

 private:
  std::coroutine_handle<promise_type> handle_;
};

template <>
class workflow_task<void> {
 public:
  struct promise_type {
    std::exception_ptr error_;
    workflow_task get_return_object() {
      return workflow_task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() {}
    void unhandled_exception() { error_ = std::current_exception(); }
  };
  using value_type = void;

  explicit workflow_task(std::coroutine_handle<promise_type> handle) : handle_(handle) {}
  workflow_task(workflow_task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  workflow_task& operator=(workflow_task&&) = delete;
  workflow_task(const workflow_task&) = delete;
  workflow_task& operator=(const workflow_task&) = delete;
  ~workflow_task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  void get() {
    if (handle_.promise().error_) {
      std::rethrow_exception(handle_.promise().error_);
    }
  }

 private:
  std::coroutine_handle<promise_type> handle_;
};

template <class T>
struct is_workflow_task : std::false_type {};
template <class R>
struct is_workflow_task<workflow_task<R>> : std::true_type {};
template <class T>
inline constexpr bool is_workflow_task_v = is_workflow_task<T>::value;

}  // namespace temporal::workflow
