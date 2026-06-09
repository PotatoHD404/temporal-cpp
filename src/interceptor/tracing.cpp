#include <temporal/interceptor/tracing.h>

#include <cstddef>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include <temporal/activity/activity.h>
#include <temporal/common/payload.h>
#include <temporal/workflow/context.h>

// Implementation of the tracing interceptor's per-call wrappers, the header
// (de)serialization, and the test-only InMemoryTracer. See tracing.h for the
// design and honesty caveats (framework only; not yet wired into client/worker).
namespace temporal::interceptor {

namespace {

// Span tag keys, mirroring the Go SDK's tracing_interceptor.go constants so
// emitted traces look the same across languages.
constexpr const char* kWorkflowIdTag = "temporalWorkflowID";
constexpr const char* kRunIdTag = "temporalRunID";
constexpr const char* kActivityIdTag = "temporalActivityID";

}  // namespace

// ---------------------------------------------------------------------------
// Header wire format. The tracer's flat map is stored as ONE JSON-encoded
// Payload (encoding "json/plain") under `header_key`. This matches how the SDK's
// default DataConverter encodes a map<string,string>, so the lead can swap in
// the real DataConverter later without changing the on-the-wire shape.
// ---------------------------------------------------------------------------
void TracingInterceptor::WriteToHeader(const std::string& header_key,
                                       const std::map<std::string, std::string>& data,
                                       Header& header) {
  if (data.empty()) {
    return;  // Nothing to propagate (mirrors Go skipping empty span data).
  }
  nlohmann::json j = data;  // object of string->string
  temporal::Payload payload;
  payload.metadata[temporal::metadata_keys::kEncoding] = temporal::encodings::kJson;
  payload.data = j.dump();
  header[header_key] = std::move(payload);
}

std::map<std::string, std::string> TracingInterceptor::ReadFromHeader(const std::string& header_key,
                                                                      const Header& header) {
  auto it = header.find(header_key);
  if (it == header.end()) {
    return {};
  }
  // Tolerate malformed/foreign payloads: a bad parse yields an empty map (the
  // span then simply has no parent), never an exception across the call path.
  nlohmann::json j = nlohmann::json::parse(it->second.data, nullptr, /*allow_exceptions=*/false);
  if (!j.is_object()) {
    return {};
  }
  std::map<std::string, std::string> out;
  for (auto field = j.begin(); field != j.end(); ++field) {
    if (field.value().is_string()) {
      out.emplace(field.key(), field.value().get<std::string>());
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Per-call wrappers.
// ---------------------------------------------------------------------------

// Outbound (from inside a workflow): start a span, inject it into the outbound
// header, then delegate. The span ends when the call returns.
// Shared per-workflow slot holding the current inbound workflow span's context,
// so outbound spans (activity/child/signal) can parent off it and stay in one trace.
struct WorkflowSpanSlot {
  const SpanContext* current = nullptr;
};

class TracingInterceptor::WorkflowOutbound : public WorkflowOutboundInterceptor {
 public:
  WorkflowOutbound(TracingInterceptor* root, std::shared_ptr<WorkflowSpanSlot> slot,
                   WorkflowOutboundInterceptor* next)
      : WorkflowOutboundInterceptor(next), root_(root), slot_(std::move(slot)) {}

  void ExecuteActivity(workflow::Context& ctx, ExecuteActivityOutboundInput& in,
                       Header& header) override {
    auto span = StartAndInject(ctx, "StartActivity", in.activity_type, header);
    next_->ExecuteActivity(ctx, in, header);
    span->End();
  }

  void ExecuteChildWorkflow(workflow::Context& ctx, ExecuteChildWorkflowInput& in,
                            Header& header) override {
    auto span = StartAndInject(ctx, "StartChildWorkflow", in.workflow_type, header);
    next_->ExecuteChildWorkflow(ctx, in, header);
    span->End();
  }

  void SignalExternalWorkflow(workflow::Context& ctx, SignalExternalWorkflowInput& in,
                              Header& header) override {
    auto span = StartAndInject(ctx, "SignalExternalWorkflow", in.signal_name, header);
    next_->SignalExternalWorkflow(ctx, in, header);
    span->End();
  }

  // CancelExternalWorkflow / UpsertSearchAttributes have no propagation header in
  // this SDK's outbound surface; fall through to the base (no span).

 private:
  std::unique_ptr<Span> StartAndInject(workflow::Context& ctx, const std::string& operation,
                                       const std::string& name, Header& header) {
    const workflow::WorkflowInfo& info = ctx.GetInfo();
    StartSpanOptions opts;
    opts.operation = operation;
    opts.name = name;
    opts.tags[kWorkflowIdTag] = info.workflow_id;
    opts.tags[kRunIdTag] = info.run_id;
    opts.parent = slot_->current;  // parent off the current workflow span (connects the trace)
    auto span = root_->tracer()->StartSpan(opts);
    WriteToHeader(root_->header_key(), root_->tracer()->Inject(*span), header);
    return span;
  }

  TracingInterceptor* root_;
  std::shared_ptr<WorkflowSpanSlot> slot_;
};

// Inbound workflow: extract a parent from the inbound header, start a span,
// install the outbound tracing wrapper in Init, then delegate.
class TracingInterceptor::WorkflowInbound : public WorkflowInboundInterceptor {
 public:
  WorkflowInbound(TracingInterceptor* root, WorkflowInboundInterceptor* next)
      : WorkflowInboundInterceptor(next), root_(root) {}

  void Init(WorkflowOutboundInterceptor* outbound) override {
    // Wrap the outbound chain so SDK-originated calls inject the current span.
    outbound_ = std::make_unique<WorkflowOutbound>(root_, slot_, outbound);
    next_->Init(outbound_.get());
  }

  temporal::Payloads ExecuteWorkflow(workflow::Context& ctx, ExecuteWorkflowInput& in,
                                     const Header& header) override {
    auto parent = ExtractParent(header);
    const workflow::WorkflowInfo& info = ctx.GetInfo();
    StartSpanOptions opts;
    opts.operation = "RunWorkflow";
    opts.name = info.workflow_type;
    opts.parent = parent ? &*parent : nullptr;
    opts.tags[kWorkflowIdTag] = info.workflow_id;
    opts.tags[kRunIdTag] = info.run_id;
    auto span = root_->tracer()->StartSpan(opts);
    slot_->current = &span->context();  // visible to the outbound wrapper while the workflow runs
    try {
      temporal::Payloads result = next_->ExecuteWorkflow(ctx, in, header);
      slot_->current = nullptr;
      span->End();
      return result;
    } catch (...) {
      slot_->current = nullptr;
      span->End(/*error=*/true);
      throw;
    }
  }

  void HandleSignal(workflow::Context& ctx, HandleSignalInput& in, const Header& header) override {
    auto parent = ExtractParent(header);
    StartSpanOptions opts;
    opts.operation = "HandleSignal";
    opts.name = in.signal_name;
    opts.parent = parent ? &*parent : nullptr;
    auto span = root_->tracer()->StartSpan(opts);
    try {
      next_->HandleSignal(ctx, in, header);
      span->End();
    } catch (...) {
      span->End(/*error=*/true);
      throw;
    }
  }

  temporal::Payloads HandleQuery(workflow::Context& ctx, HandleQueryInput& in,
                                 const Header& header) override {
    auto parent = ExtractParent(header);
    StartSpanOptions opts;
    opts.operation = "HandleQuery";
    opts.name = in.query_type;
    opts.parent = parent ? &*parent : nullptr;
    auto span = root_->tracer()->StartSpan(opts);
    try {
      temporal::Payloads result = next_->HandleQuery(ctx, in, header);
      span->End();
      return result;
    } catch (...) {
      span->End(/*error=*/true);
      throw;
    }
  }

 private:
  std::optional<SpanContext> ExtractParent(const Header& header) {
    return root_->tracer()->Extract(ReadFromHeader(root_->header_key(), header));
  }

  TracingInterceptor* root_;
  std::shared_ptr<WorkflowSpanSlot> slot_ = std::make_shared<WorkflowSpanSlot>();
  std::unique_ptr<WorkflowOutbound> outbound_;
};

// Inbound activity: extract parent from the activity's headers, span the run.
class TracingInterceptor::ActivityInbound : public ActivityInboundInterceptor {
 public:
  ActivityInbound(TracingInterceptor* root, ActivityInboundInterceptor* next)
      : ActivityInboundInterceptor(next), root_(root) {}

  temporal::Payloads ExecuteActivity(activity::Context& ctx, ExecuteActivityInput& in,
                                     const Header& header) override {
    auto parent = root_->tracer()->Extract(ReadFromHeader(root_->header_key(), header));
    const activity::ActivityInfo& info = ctx.GetInfo();
    StartSpanOptions opts;
    opts.operation = "RunActivity";
    opts.name = info.activity_type;
    opts.parent = parent ? &*parent : nullptr;
    opts.tags[kWorkflowIdTag] = info.workflow_id;
    opts.tags[kRunIdTag] = info.run_id;
    opts.tags[kActivityIdTag] = info.activity_id;
    auto span = root_->tracer()->StartSpan(opts);
    try {
      temporal::Payloads result = next_->ExecuteActivity(ctx, in, header);
      span->End();
      return result;
    } catch (...) {
      span->End(/*error=*/true);
      throw;
    }
  }

 private:
  TracingInterceptor* root_;
};

// Client outbound: start a span and inject into the outbound header.
class TracingInterceptor::ClientOutbound : public ClientOutboundInterceptor {
 public:
  ClientOutbound(TracingInterceptor* root, ClientOutboundInterceptor* next)
      : ClientOutboundInterceptor(next), root_(root) {}

  std::string StartWorkflow(StartWorkflowInput& in, Header& header) override {
    StartSpanOptions opts;
    opts.operation = "StartWorkflow";
    opts.name = in.workflow_type;
    opts.tags[kWorkflowIdTag] = in.options.id;
    auto span = root_->tracer()->StartSpan(opts);
    WriteToHeader(root_->header_key(), root_->tracer()->Inject(*span), header);
    std::string run_id = next_->StartWorkflow(in, header);
    span->End();
    return run_id;
  }

  void SignalWorkflow(SignalWorkflowInput& in, Header& header) override {
    StartSpanOptions opts;
    opts.operation = "SignalWorkflow";
    opts.name = in.signal_name;
    opts.tags[kWorkflowIdTag] = in.workflow_id;
    auto span = root_->tracer()->StartSpan(opts);
    WriteToHeader(root_->header_key(), root_->tracer()->Inject(*span), header);
    next_->SignalWorkflow(in, header);
    span->End();
  }

  temporal::Payloads QueryWorkflow(QueryWorkflowInput& in, Header& header) override {
    StartSpanOptions opts;
    opts.operation = "QueryWorkflow";
    opts.name = in.query_type;
    opts.tags[kWorkflowIdTag] = in.workflow_id;
    auto span = root_->tracer()->StartSpan(opts);
    WriteToHeader(root_->header_key(), root_->tracer()->Inject(*span), header);
    temporal::Payloads result = next_->QueryWorkflow(in, header);
    span->End();
    return result;
  }

 private:
  TracingInterceptor* root_;
};

// ---------------------------------------------------------------------------
// TracingInterceptor factory methods.
// ---------------------------------------------------------------------------
std::unique_ptr<WorkflowInboundInterceptor> TracingInterceptor::InterceptWorkflow(
    WorkflowInboundInterceptor* next) {
  return std::make_unique<WorkflowInbound>(this, next);
}

std::unique_ptr<ActivityInboundInterceptor> TracingInterceptor::InterceptActivity(
    ActivityInboundInterceptor* next) {
  return std::make_unique<ActivityInbound>(this, next);
}

std::unique_ptr<ClientOutboundInterceptor> TracingInterceptor::InterceptClient(
    ClientOutboundInterceptor* next) {
  return std::make_unique<ClientOutbound>(this, next);
}

// ---------------------------------------------------------------------------
// InMemoryTracer (test-only). Assigns incrementing ids and records spans; the
// propagation map carries (trace_id, span_id) so a child started from an
// extracted context inherits the trace and points at its parent.
// ---------------------------------------------------------------------------
class InMemoryTracer::MemSpan : public Span {
 public:
  MemSpan(InMemoryTracer* owner, std::size_t record_index, SpanContext ctx)
      : owner_(owner), record_index_(record_index), ctx_(std::move(ctx)) {}

  void SetTag(const std::string& key, const std::string& value) override {
    owner_->records_[record_index_].tags[key] = value;
  }
  void End(bool error) override {
    auto& rec = owner_->records_[record_index_];
    rec.ended = true;
    rec.error = error;
  }
  const SpanContext& context() const override { return ctx_; }

 private:
  InMemoryTracer* owner_;
  std::size_t record_index_;
  SpanContext ctx_;
};

std::unique_ptr<Span> InMemoryTracer::StartSpan(const StartSpanOptions& options) {
  Record rec;
  rec.operation = options.operation;
  rec.name = options.name;
  rec.tags = options.tags;

  std::string trace_id;
  std::string parent_span_id;
  if (options.parent != nullptr) {
    const auto& data = options.parent->data();
    if (auto it = data.find(kTraceIdKey); it != data.end()) {
      trace_id = it->second;
    }
    if (auto it = data.find(kSpanIdKey); it != data.end()) {
      parent_span_id = it->second;
    }
  }
  if (trace_id.empty()) {
    trace_id = "t" + std::to_string(next_trace_id_++);
  }
  std::string span_id = "s" + std::to_string(next_span_id_++);

  rec.trace_id = trace_id;
  rec.span_id = span_id;
  rec.parent_span_id = parent_span_id;

  std::map<std::string, std::string> ctx_data{{kTraceIdKey, trace_id}, {kSpanIdKey, span_id}};
  std::size_t index = records_.size();
  records_.push_back(std::move(rec));
  return std::make_unique<MemSpan>(this, index, SpanContext(std::move(ctx_data)));
}

std::map<std::string, std::string> InMemoryTracer::Inject(const Span& span) const {
  // The span's own context already holds (trace_id, span_id) — propagate it.
  return span.context().data();
}

std::optional<SpanContext> InMemoryTracer::Extract(
    const std::map<std::string, std::string>& data) const {
  if (data.find(kTraceIdKey) == data.end() || data.find(kSpanIdKey) == data.end()) {
    return std::nullopt;
  }
  return SpanContext(data);
}

}  // namespace temporal::interceptor
