#include <gtest/gtest.h>

#include <temporal/common/errors.h>

namespace {

// NotFoundError is an RpcError subtype: existing `catch (RpcError&)` still catches
// it (with not_found() == true), and new code can catch it directly.
TEST(Errors, NotFoundErrorIsCatchableAsRpcErrorAndDirectly) {
  bool caught_directly = false;
  try {
    throw temporal::NotFoundError("missing workflow");
  } catch (const temporal::NotFoundError&) {
    caught_directly = true;
  }
  EXPECT_TRUE(caught_directly);

  bool not_found_flag = false;
  try {
    throw temporal::NotFoundError("missing workflow");
  } catch (const temporal::RpcError& e) {  // base-class catch still works
    not_found_flag = e.not_found();
  }
  EXPECT_TRUE(not_found_flag);
}

}  // namespace
