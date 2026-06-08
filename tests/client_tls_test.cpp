#include <gtest/gtest.h>

#include <temporal/client/client.h>

namespace {

// Constructing a TLS / mTLS + API-key client must not throw: the credential
// wiring (SslCredentials, server-name override, per-call auth metadata) runs at
// construction, and the channel is lazy (no connection until the first RPC).
//
// NOTE: full mTLS / API-key behaviour is NOT exercised end to end here — the
// local test harness has no TLS-terminating Temporal server. This only proves
// the credential construction path is sound and doesn't regress the insecure path.
TEST(ClientTls, ConstructsWithTlsAndApiKey) {
  temporal::ClientOptions o;
  o.target = "example.invalid:7233";
  o.tls.enabled = true;
  o.tls.server_name = "example.invalid";
  o.tls.server_ca_cert = "";  // system trust store
  o.api_key = "test-api-key";
  EXPECT_NO_THROW({
    auto c = temporal::client::Client::Connect(o);
    (void)c;
  });
}

TEST(ClientTls, InsecureRemainsTheDefault) {
  temporal::ClientOptions o;  // tls.enabled defaults to false
  o.target = "localhost:7233";
  EXPECT_NO_THROW({
    auto c = temporal::client::Client::Connect(o);
    (void)c;
  });
}

}  // namespace
