#include <string>

#include <gtest/gtest.h>

#include <temporal/converter/data_converter.h>

namespace {

using temporal::GzipPayloadCodec;
using temporal::Payload;

TEST(GzipPayloadCodec, RoundTripsAndCompresses) {
  GzipPayloadCodec codec;

  Payload in;
  in.metadata["encoding"] = "json/plain";
  in.metadata["messageType"] = "demo";
  // Highly compressible body so the deflated form is strictly smaller.
  in.data = std::string(2048, 'a');

  const Payload encoded = codec.Encode(in);
  EXPECT_EQ(encoded.metadata.at("codec"), "gzip");
  EXPECT_NE(encoded.data, in.data);                // bytes were transformed
  EXPECT_LT(encoded.data.size(), in.data.size());  // and are smaller
  // Inner metadata is preserved through the codec.
  EXPECT_EQ(encoded.metadata.at("encoding"), "json/plain");
  EXPECT_EQ(encoded.metadata.at("messageType"), "demo");

  const Payload decoded = codec.Decode(encoded);
  EXPECT_EQ(decoded.data, in.data);                // original bytes restored
  EXPECT_EQ(decoded.metadata.count("codec"), 0U);  // marker stripped
  EXPECT_EQ(decoded.metadata.count("codec-gzip-len"), 0U);
  EXPECT_EQ(decoded.metadata.at("encoding"), "json/plain");
  EXPECT_EQ(decoded.metadata.at("messageType"), "demo");
}

TEST(GzipPayloadCodec, DecodePassesThroughUnmarkedPayload) {
  GzipPayloadCodec codec;
  Payload p;
  p.metadata["encoding"] = "json/plain";
  p.data = "{\"x\":1}";

  const Payload out = codec.Decode(p);  // no "codec"=="gzip" marker
  EXPECT_EQ(out.data, p.data);
  EXPECT_EQ(out.metadata, p.metadata);
}

}  // namespace
