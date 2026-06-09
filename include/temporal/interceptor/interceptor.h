#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <temporal/activity/activity.h>
#include <temporal/common/payload.h>
#include <temporal/workflow/context.h>

// Interceptor framework for the C++ Temporal SDK.
//
// This mirrors the Go SDK's `interceptor` package (third_party/reference/
// sdk-go/interceptor/interceptor.go), adapted to this SDK's concrete C++ types
// (workflow::Context, activity::Context, client::Client, Payload). It defines
// the abstract interfaces an interceptor implements plus no-op pass-through base
// classes (XxxInterceptorBase) so a user overrides only what they need.
//
// DESIGN — chain of "next" pointers:
//   Each inbound/outbound interceptor holds a raw `next` pointer to the interceptor
//   one step closer to the real SDK behavior. The base-class methods simply
//   delegate to `next`. A concrete interceptor overrides a method, does its work,
//   and calls the protected `next_` (or, for terminal/root interceptors, performs
//   the real call). The chain terminates at a root interceptor the SDK installs
//   that actually executes the workflow/activity/client call.
//
// IMPORTANT (honesty): this is a FRAMEWORK ONLY. Nothing here is yet wired into
// client.cpp / worker_impl.cpp / the engine — installing the chain and invoking
// it around real workflow/activity/client calls is the lead's follow-up. Until
// that wiring exists these interfaces have no runtime effect.
namespace temporal::interceptor {

// Context-propagation headers carried alongside workflow/activity/client calls.
// Identical in shape to the maps already used across the SDK
// (StartWorkflowOptions::headers, WorkflowInfo::headers, ActivityInfo::headers):
// a string key to an opaque Payload. Interceptors read/mutate this to propagate
// out-of-band context (trace spans, baggage, auth) across service boundaries.
using Header = std::map<std::string, temporal::Payload>;

// ---------------------------------------------------------------------------
// Inputs — small structs passed to inbound/outbound interceptor methods, so the
// surface can grow without breaking signatures (mirrors the Go *Input structs).
// Args are carried as already-encoded Payloads (decode happens deeper in the
// SDK, where the concrete argument types are known).
// ---------------------------------------------------------------------------

struct ExecuteWorkflowInput {
  temporal::Payloads args;
};

struct HandleSignalInput {
  std::string signal_name;
  temporal::Payloads args;
};

struct HandleQueryInput {
  std::string query_type;
  temporal::Payloads args;
};

struct ExecuteActivityInput {
  temporal::Payloads args;
};

// Outbound (SDK-originated, from inside a workflow) call inputs.
struct ExecuteActivityOutboundInput {
  std::string activity_type;
  temporal::ActivityOptions options;
  temporal::Payloads args;
};

struct ExecuteChildWorkflowInput {
  std::string workflow_type;
  temporal::ChildWorkflowOptions options;
  temporal::Payloads args;
};

struct SignalExternalWorkflowInput {
  std::string workflow_id;
  std::string run_id;  // empty => latest run
  std::string signal_name;
  temporal::Payloads args;
};

struct CancelExternalWorkflowInput {
  std::string workflow_id;
  std::string run_id;  // empty => latest run
};

struct UpsertSearchAttributesInput {
  std::map<std::string, temporal::Payload> attributes;
};

// Client-originated call inputs (the workflow-specific subset the SDK supports).
struct StartWorkflowInput {
  std::string workflow_type;
  temporal::StartWorkflowOptions options;
  temporal::Payloads args;
};

struct SignalWorkflowInput {
  std::string workflow_id;
  std::string run_id;  // empty => latest run
  std::string signal_name;
  temporal::Payloads args;
};

struct QueryWorkflowInput {
  std::string workflow_id;
  std::string run_id;  // empty => latest run
  std::string query_type;
  temporal::Payloads args;
};

// ---------------------------------------------------------------------------
// WorkflowOutboundInterceptor — calls a workflow makes back into the SDK.
// Mirrors Go's WorkflowOutboundInterceptor (the side-effecting subset this SDK
// supports: activities, child workflows, signal/cancel external, upsert SA).
// ---------------------------------------------------------------------------
class WorkflowOutboundInterceptor {
 public:
  WorkflowOutboundInterceptor() = default;
  explicit WorkflowOutboundInterceptor(WorkflowOutboundInterceptor* next) : next_(next) {}
  virtual ~WorkflowOutboundInterceptor() = default;
  WorkflowOutboundInterceptor(const WorkflowOutboundInterceptor&) = delete;
  WorkflowOutboundInterceptor& operator=(const WorkflowOutboundInterceptor&) = delete;
  WorkflowOutboundInterceptor(WorkflowOutboundInterceptor&&) = delete;
  WorkflowOutboundInterceptor& operator=(WorkflowOutboundInterceptor&&) = delete;

  // Mutable header for the outbound call. Implementations write propagation data
  // here (e.g. a serialized span) before delegating; the SDK attaches it to the
  // resulting activity/child-workflow/signal so the callee can read it inbound.
  virtual void ExecuteActivity(workflow::Context& ctx, ExecuteActivityOutboundInput& in,
                               Header& header) {
    next_->ExecuteActivity(ctx, in, header);
  }
  virtual void ExecuteChildWorkflow(workflow::Context& ctx, ExecuteChildWorkflowInput& in,
                                    Header& header) {
    next_->ExecuteChildWorkflow(ctx, in, header);
  }
  virtual void SignalExternalWorkflow(workflow::Context& ctx, SignalExternalWorkflowInput& in,
                                      Header& header) {
    next_->SignalExternalWorkflow(ctx, in, header);
  }
  virtual void CancelExternalWorkflow(workflow::Context& ctx, CancelExternalWorkflowInput& in) {
    next_->CancelExternalWorkflow(ctx, in);
  }
  virtual void UpsertSearchAttributes(workflow::Context& ctx, UpsertSearchAttributesInput& in) {
    next_->UpsertSearchAttributes(ctx, in);
  }

 protected:
  WorkflowOutboundInterceptor* next_ = nullptr;
};

// No-op pass-through; identical to the bare base. Provided for symmetry with the
// Go SDK's XxxInterceptorBase naming and as the recommended base to subclass.
using WorkflowOutboundInterceptorBase = WorkflowOutboundInterceptor;

// ---------------------------------------------------------------------------
// WorkflowInboundInterceptor — server-originated workflow calls. Init lets an
// implementation wrap the outbound interceptor before the next link sees it.
// ---------------------------------------------------------------------------
class WorkflowInboundInterceptor {
 public:
  WorkflowInboundInterceptor() = default;
  explicit WorkflowInboundInterceptor(WorkflowInboundInterceptor* next) : next_(next) {}
  virtual ~WorkflowInboundInterceptor() = default;
  WorkflowInboundInterceptor(const WorkflowInboundInterceptor&) = delete;
  WorkflowInboundInterceptor& operator=(const WorkflowInboundInterceptor&) = delete;
  WorkflowInboundInterceptor(WorkflowInboundInterceptor&&) = delete;
  WorkflowInboundInterceptor& operator=(WorkflowInboundInterceptor&&) = delete;

  // First call: an implementation may wrap `outbound` and must pass the (possibly
  // wrapped) interceptor down the chain. The default forwards unchanged.
  virtual void Init(WorkflowOutboundInterceptor* outbound) {
    if (next_ != nullptr) {
      next_->Init(outbound);
    }
  }

  // Returns the workflow's result payloads. `header` is the inbound header
  // (read-only inbound — extract propagation context here).
  virtual temporal::Payloads ExecuteWorkflow(workflow::Context& ctx, ExecuteWorkflowInput& in,
                                             const Header& header) {
    return next_->ExecuteWorkflow(ctx, in, header);
  }
  virtual void HandleSignal(workflow::Context& ctx, HandleSignalInput& in, const Header& header) {
    next_->HandleSignal(ctx, in, header);
  }
  virtual temporal::Payloads HandleQuery(workflow::Context& ctx, HandleQueryInput& in,
                                         const Header& header) {
    return next_->HandleQuery(ctx, in, header);
  }

 protected:
  WorkflowInboundInterceptor* next_ = nullptr;
};

using WorkflowInboundInterceptorBase = WorkflowInboundInterceptor;

// ---------------------------------------------------------------------------
// ActivityInboundInterceptor — server-originated activity calls.
// ---------------------------------------------------------------------------
class ActivityInboundInterceptor {
 public:
  ActivityInboundInterceptor() = default;
  explicit ActivityInboundInterceptor(ActivityInboundInterceptor* next) : next_(next) {}
  virtual ~ActivityInboundInterceptor() = default;
  ActivityInboundInterceptor(const ActivityInboundInterceptor&) = delete;
  ActivityInboundInterceptor& operator=(const ActivityInboundInterceptor&) = delete;
  ActivityInboundInterceptor(ActivityInboundInterceptor&&) = delete;
  ActivityInboundInterceptor& operator=(ActivityInboundInterceptor&&) = delete;

  // `header` carries propagation context from the scheduling workflow (it equals
  // activity::Context::GetInfo().headers); extract it here.
  virtual temporal::Payloads ExecuteActivity(activity::Context& ctx, ExecuteActivityInput& in,
                                             const Header& header) {
    return next_->ExecuteActivity(ctx, in, header);
  }

 protected:
  ActivityInboundInterceptor* next_ = nullptr;
};

using ActivityInboundInterceptorBase = ActivityInboundInterceptor;

// ---------------------------------------------------------------------------
// ClientOutboundInterceptor — workflow-specific calls a Client makes.
// ---------------------------------------------------------------------------
class ClientOutboundInterceptor {
 public:
  ClientOutboundInterceptor() = default;
  explicit ClientOutboundInterceptor(ClientOutboundInterceptor* next) : next_(next) {}
  virtual ~ClientOutboundInterceptor() = default;
  ClientOutboundInterceptor(const ClientOutboundInterceptor&) = delete;
  ClientOutboundInterceptor& operator=(const ClientOutboundInterceptor&) = delete;
  ClientOutboundInterceptor(ClientOutboundInterceptor&&) = delete;
  ClientOutboundInterceptor& operator=(ClientOutboundInterceptor&&) = delete;

  // Each takes the mutable outbound `header` so implementations inject context
  // before the SDK issues the RPC. StartWorkflow returns the started run_id;
  // QueryWorkflow returns the query result payloads.
  virtual std::string StartWorkflow(StartWorkflowInput& in, Header& header) {
    return next_->StartWorkflow(in, header);
  }
  virtual void SignalWorkflow(SignalWorkflowInput& in, Header& header) {
    next_->SignalWorkflow(in, header);
  }
  virtual temporal::Payloads QueryWorkflow(QueryWorkflowInput& in, Header& header) {
    return next_->QueryWorkflow(in, header);
  }

 protected:
  ClientOutboundInterceptor* next_ = nullptr;
};

using ClientOutboundInterceptorBase = ClientOutboundInterceptor;

// ---------------------------------------------------------------------------
// Interceptor (a.k.a. WorkerInterceptor) — top-level factory. The SDK calls
// these to wrap the next inbound interceptor when a workflow/activity starts, or
// the next client outbound interceptor when a client is built. An implementation
// returns a new interceptor whose `next` is the one passed in (classic chain).
//
// Ownership: returned pointers are owned by the SDK-side chain builder, which
// keeps them alive for the duration of the workflow task / activity task /
// client. The provided InterceptorBase returns `next` unchanged (no wrapping).
// ---------------------------------------------------------------------------
class Interceptor {
 public:
  Interceptor() = default;
  virtual ~Interceptor() = default;
  Interceptor(const Interceptor&) = delete;
  Interceptor& operator=(const Interceptor&) = delete;
  Interceptor(Interceptor&&) = delete;
  Interceptor& operator=(Interceptor&&) = delete;

  // Worker side. Each returns an interceptor to insert in front of `next`. The
  // returned object must outlive the call it wraps; implementations typically
  // allocate and hand ownership to the caller (see MakeChain below for the
  // common pattern). Returning `next` unchanged means "do not intercept".
  virtual std::unique_ptr<WorkflowInboundInterceptor> InterceptWorkflow(
      WorkflowInboundInterceptor* next) {
    (void)next;
    return nullptr;  // nullptr => no wrapper; chain builder keeps `next`.
  }
  virtual std::unique_ptr<ActivityInboundInterceptor> InterceptActivity(
      ActivityInboundInterceptor* next) {
    (void)next;
    return nullptr;
  }

  // Client side.
  virtual std::unique_ptr<ClientOutboundInterceptor> InterceptClient(
      ClientOutboundInterceptor* next) {
    (void)next;
    return nullptr;
  }
};

// Alias matching the Go SDK's WorkerInterceptor name (Interceptor combines the
// worker + client factory surface here).
using WorkerInterceptor = Interceptor;

// A no-op top-level interceptor: every Intercept* returns nullptr ("don't wrap").
// Subclass this and override only the factory you care about.
using InterceptorBase = Interceptor;

// ---------------------------------------------------------------------------
// Chain composition helpers.
//
// Given an ordered list of Interceptor factories and a terminal interceptor that
// performs the real SDK behavior, build a linked chain. The FIRST factory in the
// list becomes the OUTERMOST interceptor (called first), matching the Go SDK,
// where interceptors are applied in order so options-order == call-order.
//
// The returned Chain owns every wrapper it allocated (so they outlive use) and
// exposes head(): the outermost interceptor to invoke. The terminal interceptor
// is NOT owned by the chain (the SDK owns it).
// ---------------------------------------------------------------------------

template <class Inbound>
class InterceptorChain {
 public:
  InterceptorChain() = default;
  // Non-copyable (owns unique_ptrs); movable.
  InterceptorChain(const InterceptorChain&) = delete;
  InterceptorChain& operator=(const InterceptorChain&) = delete;
  InterceptorChain(InterceptorChain&&) = default;
  InterceptorChain& operator=(InterceptorChain&&) = default;
  ~InterceptorChain() = default;

  Inbound* head() const { return head_; }

  // Used by the builders below.
  void set_head(Inbound* head) { head_ = head; }
  void adopt(std::unique_ptr<Inbound> wrapper) { owned_.push_back(std::move(wrapper)); }

 private:
  Inbound* head_ = nullptr;
  std::vector<std::unique_ptr<Inbound>> owned_;
};

// Build a workflow-inbound chain. `terminal` is the SDK-owned interceptor that
// runs the real workflow; `factories` are applied in order (front = outermost).
inline InterceptorChain<WorkflowInboundInterceptor> BuildWorkflowInboundChain(
    const std::vector<Interceptor*>& factories, WorkflowInboundInterceptor* terminal) {
  InterceptorChain<WorkflowInboundInterceptor> chain;
  WorkflowInboundInterceptor* next = terminal;
  // Walk factories back-to-front so the first factory ends up outermost.
  for (auto it = factories.rbegin(); it != factories.rend(); ++it) {
    std::unique_ptr<WorkflowInboundInterceptor> wrapper = (*it)->InterceptWorkflow(next);
    if (wrapper) {
      next = wrapper.get();
      chain.adopt(std::move(wrapper));
    }
  }
  chain.set_head(next);
  return chain;
}

inline InterceptorChain<ActivityInboundInterceptor> BuildActivityInboundChain(
    const std::vector<Interceptor*>& factories, ActivityInboundInterceptor* terminal) {
  InterceptorChain<ActivityInboundInterceptor> chain;
  ActivityInboundInterceptor* next = terminal;
  for (auto it = factories.rbegin(); it != factories.rend(); ++it) {
    std::unique_ptr<ActivityInboundInterceptor> wrapper = (*it)->InterceptActivity(next);
    if (wrapper) {
      next = wrapper.get();
      chain.adopt(std::move(wrapper));
    }
  }
  chain.set_head(next);
  return chain;
}

inline InterceptorChain<ClientOutboundInterceptor> BuildClientOutboundChain(
    const std::vector<Interceptor*>& factories, ClientOutboundInterceptor* terminal) {
  InterceptorChain<ClientOutboundInterceptor> chain;
  ClientOutboundInterceptor* next = terminal;
  for (auto it = factories.rbegin(); it != factories.rend(); ++it) {
    std::unique_ptr<ClientOutboundInterceptor> wrapper = (*it)->InterceptClient(next);
    if (wrapper) {
      next = wrapper.get();
      chain.adopt(std::move(wrapper));
    }
  }
  chain.set_head(next);
  return chain;
}

}  // namespace temporal::interceptor
