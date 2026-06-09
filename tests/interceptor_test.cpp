#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <temporal/activity/activity.h>
#include <temporal/common/payload.h>
#include <temporal/converter/data_converter.h>
#include <temporal/interceptor/interceptor.h>
#include <temporal/interceptor/tracing.h>

namespace {

namespace ic = temporal::interceptor;
namespace activity = temporal::activity;

// Records the order in which interceptors are entered/exited, shared across the
// chain so we can assert nesting (outer enters first, exits last).
struct CallLog {
  std::vector<std::string> events;
};

// A spying ActivityInbound wrapper: logs enter/exit around delegating to next.
class SpyActivityInbound : public ic::ActivityInboundInterceptor {
 public:
  SpyActivityInbound(std::string tag, CallLog* log, ic::ActivityInboundInterceptor* next)
      : ic::ActivityInboundInterceptor(next), tag_(std::move(tag)), log_(log) {}

  temporal::Payloads ExecuteActivity(activity::Context& ctx, ic::ExecuteActivityInput& in,
                                     const ic::Header& header) override {
    log_->events.push_back(tag_ + ":enter");
    temporal::Payloads out = next_->ExecuteActivity(ctx, in, header);
    log_->events.push_back(tag_ + ":exit");
    return out;
  }

 private:
  std::string tag_;
  CallLog* log_;
};

// Factory that produces a SpyActivityInbound (and only that).
class SpyFactory : public ic::Interceptor {
 public:
  SpyFactory(std::string tag, CallLog* log) : tag_(std::move(tag)), log_(log) {}
  std::unique_ptr<ic::ActivityInboundInterceptor> InterceptActivity(
      ic::ActivityInboundInterceptor* next) override {
    return std::make_unique<SpyActivityInbound>(tag_, log_, next);
  }

 private:
  std::string tag_;
  CallLog* log_;
};

// Terminal activity interceptor: the "real" behavior. Records the header size so
// we can prove the header survived the chain, and logs its run.
class TerminalActivityInbound : public ic::ActivityInboundInterceptor {
 public:
  explicit TerminalActivityInbound(CallLog* log) : log_(log) {}
  temporal::Payloads ExecuteActivity(activity::Context& ctx, ic::ExecuteActivityInput& in,
                                     const ic::Header& header) override {
    (void)ctx;
    (void)in;
    log_->events.push_back("terminal");
    last_header_keys_ = static_cast<int>(header.size());
    return temporal::Payloads{};
  }
  int last_header_keys_ = -1;

 private:
  CallLog* log_;
};

activity::Context MakeActivityContext(const temporal::DataConverter* dc, const ic::Header& header) {
  activity::ActivityInfo info;
  info.activity_id = "act-1";
  info.activity_type = "DoThing";
  info.workflow_id = "wf-1";
  info.run_id = "run-1";
  info.headers = header;  // mirrors how the SDK surfaces inbound headers
  return activity::Context(info, dc);
}

// ---- POSITIVE: two-interceptor chain preserves order + passes through --------

TEST(Interceptor, ActivityInboundChainOrderingAndPassThrough) {
  const auto dc = temporal::DataConverter::Default();
  CallLog log;
  TerminalActivityInbound terminal(&log);
  SpyFactory outer("A", &log);
  SpyFactory inner("B", &log);

  // First factory is outermost: A wraps B wraps terminal.
  std::vector<ic::Interceptor*> factories{&outer, &inner};
  auto chain = ic::BuildActivityInboundChain(factories, &terminal);

  ic::Header header;  // empty inbound header
  auto act_ctx = MakeActivityContext(dc.get(), header);
  ic::ExecuteActivityInput in;
  chain.head()->ExecuteActivity(act_ctx, in, header);

  // Outer enters first, exits last; terminal runs in the middle (pass-through).
  const std::vector<std::string> expected{"A:enter", "B:enter", "terminal", "B:exit", "A:exit"};
  EXPECT_EQ(log.events, expected);
}

// ---- NEGATIVE: no factories => head() is the terminal, zero wrapping --------

TEST(Interceptor, EmptyChainHeadIsTerminalAndNoWrapping) {
  const auto dc = temporal::DataConverter::Default();
  CallLog log;
  TerminalActivityInbound terminal(&log);

  std::vector<ic::Interceptor*> none;
  auto chain = ic::BuildActivityInboundChain(none, &terminal);
  EXPECT_EQ(chain.head(), &terminal);  // head IS the terminal, nothing inserted

  ic::Header header;
  auto act_ctx = MakeActivityContext(dc.get(), header);
  ic::ExecuteActivityInput in;
  chain.head()->ExecuteActivity(act_ctx, in, header);
  EXPECT_EQ(log.events, (std::vector<std::string>{"terminal"}));  // no spies ran
}

// A base/no-op Interceptor returns nullptr from every factory => still no wrap.
TEST(Interceptor, NoOpInterceptorDoesNotWrap) {
  const auto dc = temporal::DataConverter::Default();
  CallLog log;
  TerminalActivityInbound terminal(&log);
  ic::InterceptorBase noop;  // base Interceptor: InterceptActivity returns nullptr

  std::vector<ic::Interceptor*> factories{&noop};
  auto chain = ic::BuildActivityInboundChain(factories, &terminal);
  EXPECT_EQ(chain.head(), &terminal);
}

// ---- POSITIVE: TracingInterceptor injects on client-out, extracts on act-in --

TEST(Tracing, InjectThenExtractRoundTripViaHeader) {
  const auto dc = temporal::DataConverter::Default();
  ic::InMemoryTracer tracer;
  ic::TracingInterceptor tracing(&tracer);

  // --- Client outbound side: StartWorkflow injects a span into the header. ---
  class TerminalClientOut : public ic::ClientOutboundInterceptor {
   public:
    std::string StartWorkflow(ic::StartWorkflowInput& in, ic::Header& header) override {
      (void)in;
      captured = header;
      return "run-xyz";
    }
    ic::Header captured;
  } terminal_client;

  std::vector<ic::Interceptor*> cfac{&tracing};
  auto client_chain = ic::BuildClientOutboundChain(cfac, &terminal_client);

  ic::StartWorkflowInput swi;
  swi.workflow_type = "MyWorkflow";
  swi.options.id = "wf-42";
  ic::Header out_header;
  std::string run_id = client_chain.head()->StartWorkflow(swi, out_header);
  EXPECT_EQ(run_id, "run-xyz");

  // The tracing interceptor wrote the tracer payload under the default key.
  ASSERT_TRUE(out_header.count(ic::kDefaultTracerHeaderKey));
  EXPECT_EQ(terminal_client.captured.count(ic::kDefaultTracerHeaderKey), 1u);

  // The parent (client) span exists and is a root (no parent_span_id).
  ASSERT_EQ(tracer.records().size(), 1u);
  const std::string parent_span_id = tracer.records()[0].span_id;
  const std::string parent_trace_id = tracer.records()[0].trace_id;
  EXPECT_TRUE(tracer.records()[0].parent_span_id.empty());

  // --- Activity inbound side: extract the parent from the propagated header. --
  class TerminalActIn : public ic::ActivityInboundInterceptor {
   public:
    temporal::Payloads ExecuteActivity(activity::Context& ctx, ic::ExecuteActivityInput& in,
                                       const ic::Header& header) override {
      (void)ctx;
      (void)in;
      (void)header;
      return temporal::Payloads{};
    }
  } terminal_act;

  std::vector<ic::Interceptor*> afac{&tracing};
  auto act_chain = ic::BuildActivityInboundChain(afac, &terminal_act);

  auto act_ctx = MakeActivityContext(dc.get(), out_header);  // header carries the span
  ic::ExecuteActivityInput ain;
  act_chain.head()->ExecuteActivity(act_ctx, ain, out_header);

  // A second (activity) span was started; it inherits the trace and parents off
  // the injected client span — proving inject -> header -> extract linked them.
  ASSERT_EQ(tracer.records().size(), 2u);
  const auto& act_span = tracer.records()[1];
  EXPECT_EQ(act_span.operation, "RunActivity");
  EXPECT_EQ(act_span.trace_id, parent_trace_id);
  EXPECT_EQ(act_span.parent_span_id, parent_span_id);
  EXPECT_TRUE(act_span.ended);
}

// ---- NEGATIVE: no inbound header => extracted parent is null (root span) -----

TEST(Tracing, NoHeaderYieldsRootSpan) {
  const auto dc = temporal::DataConverter::Default();
  ic::InMemoryTracer tracer;
  ic::TracingInterceptor tracing(&tracer);

  class TerminalActIn : public ic::ActivityInboundInterceptor {
   public:
    temporal::Payloads ExecuteActivity(activity::Context&, ic::ExecuteActivityInput&,
                                       const ic::Header&) override {
      return temporal::Payloads{};
    }
  } terminal_act;

  std::vector<ic::Interceptor*> afac{&tracing};
  auto act_chain = ic::BuildActivityInboundChain(afac, &terminal_act);

  ic::Header empty_header;  // nothing to extract
  auto act_ctx = MakeActivityContext(dc.get(), empty_header);
  ic::ExecuteActivityInput ain;
  act_chain.head()->ExecuteActivity(act_ctx, ain, empty_header);

  ASSERT_EQ(tracer.records().size(), 1u);
  EXPECT_TRUE(tracer.records()[0].parent_span_id.empty());  // root, no parent

  // And Extract of an empty/garbage map returns nullopt directly.
  EXPECT_FALSE(tracer.Extract({}).has_value());
  EXPECT_FALSE(tracer.Extract({{"unrelated", "x"}}).has_value());

  // ReadFromHeader tolerates a malformed payload (no throw, empty map).
  ic::Header bad;
  temporal::Payload junk;
  junk.data = "{not json";
  bad[ic::kDefaultTracerHeaderKey] = junk;
  EXPECT_TRUE(ic::TracingInterceptor::ReadFromHeader(ic::kDefaultTracerHeaderKey, bad).empty());
}

}  // namespace
