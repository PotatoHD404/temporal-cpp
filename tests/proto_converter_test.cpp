#include <string>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>

#include "temporal/api/common/v1/message.pb.h"

namespace {

// A protobuf message round-trips through the data converter as binary/protobuf.
TEST(ProtoConverter, RoundTripsProtoMessage) {
  auto dc = temporal::DataConverter::Default();

  temporal::api::common::v1::WorkflowExecution exec;
  exec.set_workflow_id("wf-123");
  exec.set_run_id("run-456");

  const temporal::Payload p = dc->ToPayload(exec);
  EXPECT_EQ(p.metadata.at("encoding"), "binary/protobuf");
  EXPECT_EQ(p.metadata.at("messageType"), "temporal.api.common.v1.WorkflowExecution");

  const auto decoded = dc->FromPayload<temporal::api::common::v1::WorkflowExecution>(p);
  EXPECT_EQ(decoded.workflow_id(), "wf-123");
  EXPECT_EQ(decoded.run_id(), "run-456");
}

// The proto branch must not disturb ordinary (non-proto) values, which still use JSON.
TEST(ProtoConverter, NonProtoValueStillUsesJson) {
  auto dc = temporal::DataConverter::Default();
  const temporal::Payload p = dc->ToPayload(std::string("hello"));
  EXPECT_EQ(p.metadata.at("encoding"), "json/plain");
  EXPECT_EQ(dc->FromPayload<std::string>(p), "hello");

  const temporal::Payload pi = dc->ToPayload(42);
  EXPECT_EQ(pi.metadata.at("encoding"), "json/plain");
  EXPECT_EQ(dc->FromPayload<int>(pi), 42);
}

// Decoding a non-proto payload as a proto type is rejected (wrong encoding).
TEST(ProtoConverter, RejectsWrongEncoding) {
  auto dc = temporal::DataConverter::Default();
  const temporal::Payload json = dc->ToPayload(std::string("not-proto"));
  EXPECT_THROW(dc->FromPayload<temporal::api::common::v1::WorkflowExecution>(json), std::exception);
}

}  // namespace
