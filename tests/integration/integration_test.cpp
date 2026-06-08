// Integration tests: exercise the implemented surface end-to-end against a real
// Temporal server. Gated behind TEMPORAL_INTEGRATION=1 (and TEMPORAL_ADDRESS,
// default localhost:7233) so the default `ctest` run needs no server.
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <temporal/temporal.h>

namespace {

using namespace std::chrono_literals;

// ---- activities ----------------------------------------------------------
std::string EchoActivity(temporal::activity::Context&, std::string s) { return s; }

int AddOneActivity(temporal::activity::Context&, int n) { return n + 1; }

std::string BoomActivity(temporal::activity::Context&, std::string) {
  throw temporal::ApplicationError("boom", "BoomError");
}

// ---- workflows -----------------------------------------------------------
std::string SleepWorkflow(temporal::workflow::Context& ctx, int millis) {
  ctx.Sleep(std::chrono::milliseconds(millis));  // exercises the timer path
  return "slept";
}

std::string EchoWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  return ctx.ExecuteActivity<std::string>(o, "Echo", s).Get();
}

int ParallelWorkflow(temporal::workflow::Context& ctx, int base) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  auto f1 = ctx.ExecuteActivity<int>(o, "AddOne", base);        // all three scheduled
  auto f2 = ctx.ExecuteActivity<int>(o, "AddOne", base + 10);   // before any Get()
  auto f3 = ctx.ExecuteActivity<int>(o, "AddOne", base + 100);
  return f1.Get() + f2.Get() + f3.Get();
}

std::string FailWorkflow(temporal::workflow::Context& ctx, std::string s) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = 10s;
  o.retry_policy.maximum_attempts = 1;  // fail fast: proves RetryPolicy is wired
  o.retry_policy_set = true;
  return ctx.ExecuteActivity<std::string>(o, "Boom", s).Get();
}

std::string LongSleepWorkflow(temporal::workflow::Context& ctx, int) {
  ctx.Sleep(60s);
  return "done";
}

// ---- harness -------------------------------------------------------------
std::atomic<int> g_seq{0};

std::string UniqueTaskQueue(const std::string& base) {
  return "itest-" + base + "-" + std::to_string(g_seq.fetch_add(1));
}

class IntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (std::getenv("TEMPORAL_INTEGRATION") == nullptr) {
      GTEST_SKIP() << "set TEMPORAL_INTEGRATION=1 and run `temporal server start-dev` to enable";
    }
    const char* addr = std::getenv("TEMPORAL_ADDRESS");
    temporal::ClientOptions opt;
    opt.target = (addr != nullptr) ? addr : "localhost:7233";
    client_ = std::make_unique<temporal::client::Client>(temporal::client::Client::Connect(opt));
  }

  std::unique_ptr<temporal::client::Client> client_;
};

TEST_F(IntegrationTest, TimerWorkflowCompletes) {
  const auto tq = UniqueTaskQueue("timer");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("SleepWorkflow", SleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "SleepWorkflow", 500);
  EXPECT_EQ(handle.Result<std::string>(), "slept");
  worker.Stop();
}

TEST_F(IntegrationTest, SingleActivityRoundTrip) {
  const auto tq = UniqueTaskQueue("echo");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
  worker.RegisterActivity("Echo", EchoActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "EchoWorkflow", std::string("ping"));
  EXPECT_EQ(handle.Result<std::string>(), "ping");
  worker.Stop();
}

TEST_F(IntegrationTest, ParallelActivitiesAllResolve) {
  const auto tq = UniqueTaskQueue("parallel");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("ParallelWorkflow", ParallelWorkflow);
  worker.RegisterActivity("AddOne", AddOneActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "ParallelWorkflow", 0);
  EXPECT_EQ(handle.Result<int>(), (0 + 1) + (10 + 1) + (100 + 1));  // 113
  worker.Stop();
}

TEST_F(IntegrationTest, ActivityFailurePropagatesWithMaxOneAttempt) {
  const auto tq = UniqueTaskQueue("fail");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("FailWorkflow", FailWorkflow);
  worker.RegisterActivity("Boom", BoomActivity);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "FailWorkflow", std::string("x"));
  // Without RetryPolicy wiring the default policy would retry forever and this
  // would hang until the ctest timeout; maximum_attempts=1 makes it fail fast.
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  worker.Stop();
}

TEST_F(IntegrationTest, TerminateMakesResultThrow) {
  const auto tq = UniqueTaskQueue("terminate");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("LongSleepWorkflow", LongSleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "LongSleepWorkflow", 0);
  handle.Terminate("integration test");
  EXPECT_THROW(handle.Result<std::string>(), temporal::WorkflowFailedError);
  worker.Stop();
}

// Until workflow-side signal/cancel handling exists, this only asserts the client
// RPCs succeed against a running workflow (see docs/ROADMAP.md).
TEST_F(IntegrationTest, SignalAndCancelRpcsSucceed) {
  const auto tq = UniqueTaskQueue("signal");
  temporal::worker::Worker worker(*client_, tq);
  worker.RegisterWorkflow("LongSleepWorkflow", LongSleepWorkflow);
  worker.Start();

  temporal::StartWorkflowOptions o;
  o.task_queue = tq;
  auto handle = client_->StartWorkflow(o, "LongSleepWorkflow", 0);
  const auto dc = temporal::DataConverter::Default();
  EXPECT_NO_THROW(handle.Signal("ping", dc->ToPayloads(std::string("hi"))));
  EXPECT_NO_THROW(handle.Cancel());
  handle.Terminate("cleanup");
  worker.Stop();
}

}  // namespace
