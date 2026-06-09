# temporal-cpp-sdk

An **experimental, native C++ SDK for [Temporal](https://temporal.io)** — a from-scratch
port modeled on the official [Go SDK](https://github.com/temporalio/sdk-go), talking directly
to the Temporal frontend over gRPC with its own workflow replay engine. No Rust `sdk-core`
dependency.

📖 **Documentation (EN + RU): https://potatohd404.github.io/temporal-cpp-sdk/**

> ⚠️ **Status: experimental.** This is not an official Temporal SDK and is not affiliated with
> Temporal Technologies. It implements a substantial, fully-tested **core** of the Temporal
> programming model — but it is **not at parity** with the official SDKs (the full surface is
> years of work). The honest, itemized capability matrix is on the
> [parity page](https://potatohd404.github.io/temporal-cpp-sdk/parity).

## What it can do

Workflows & activities, with a deterministic replay engine:

- **Workflows** — typed orchestration on a stackful-coroutine engine with a sticky cache;
  timers, child workflows (incl. parent-close policy), continue-as-new, selectors,
  `SideEffect` / `MutableSideEffect` / `GetVersion`, search attributes / memo, cancellation
  scopes, and a C++20 `co_await` authoring mode.
- **Activities** — typed execution, server-driven retries, heartbeating (auto-throttled),
  local activities, and async / manual completion.
- **Message passing** — signals, queries, updates (with validators), and typed
  `SignalRef` / `QueryRef` / `UpdateRef` handles.
- **Nexus** — endpoint management + operation calls (`ExecuteNexusOperation`) + a worker
  Nexus handler (`RegisterNexusOperation`).
- **Client & worker** — start / signal / query / update / cancel / terminate, schedules
  (interval + cron), batch operations, visibility queries, worker versioning, sessions,
  TLS / mTLS / API-key auth, interceptors, metrics & tracing, poller autoscaling, and a
  graceful drain.
- **Testing** — an offline history replayer (`Worker::ReplayWorkflowHistory`) and a
  time-skipping `testing::TestWorkflowEnvironment` (against the Temporal test server).

See the [docs site](https://potatohd404.github.io/temporal-cpp-sdk/) for guides and the
[parity matrix](https://potatohd404.github.io/temporal-cpp-sdk/parity) for the precise,
honest accounting of what is and isn't there yet.

## Why native (and how it relates to the other SDKs)

Temporal's SDKs come in two flavors:

- **Native** (Go, Java): implement the gRPC client *and* the determinism-critical workflow
  state-machine / history-replay engine directly in the host language.
- **Core-based** (Python, TypeScript, .NET, Ruby): delegate that engine to the Rust
  [`sdk-core`](https://github.com/temporalio/sdk-core) and only implement a thin lang-side
  runtime + data converter + public API.

This project takes the **native** route, mirroring the Go SDK's structure and developer
experience in idiomatic C++. See the
[Architecture page](https://potatohd404.github.io/temporal-cpp-sdk/architecture) for the
design and the determinism model.

## Public API at a glance

The API deliberately reads like the Go SDK:

```cpp
#include <temporal/temporal.h>
#include <chrono>
#include <string>

// An activity: runs in real time, may do I/O.
std::string ComposeGreeting(temporal::activity::Context& ctx, std::string name) {
  return "Hello, " + name + "!";
}

// A workflow: deterministic orchestration. `Get()` blocks (parks the workflow)
// until the activity resolves on a later workflow task.
std::string GreetWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);
  return ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", name).Get();
}

int main() {
  auto client = temporal::client::Client::Connect({.target = "localhost:7233"});

  temporal::worker::Worker worker(client, "hello-world");
  worker.RegisterWorkflow("GreetWorkflow", GreetWorkflow);
  worker.RegisterActivity("ComposeGreeting", ComposeGreeting);
  worker.Start();

  temporal::StartWorkflowOptions opts;
  opts.task_queue = "hello-world";
  auto handle = client.StartWorkflow(opts, "GreetWorkflow", std::string("Temporal"));
  std::cout << handle.Result<std::string>() << "\n";  // -> Hello, Temporal!

  worker.Stop();
}
```

New to Temporal? Start with the
[Getting started](https://potatohd404.github.io/temporal-cpp-sdk/getting-started) guide and
the [tutorial](https://potatohd404.github.io/temporal-cpp-sdk/tutorial).

## Requirements

- C++20 compiler (Apple Clang 21 / recent Clang or GCC)
- CMake ≥ 3.21
- gRPC + Protobuf C++, nlohmann-json — on macOS: `brew install grpc protobuf nlohmann-json`
- For the end-to-end example: the Temporal CLI dev server — `brew install temporal`
- GoogleTest is fetched automatically (CMake `FetchContent`)

The `temporalio/api` protobuf definitions are vendored as a git submodule under
`third_party/api` and compiled at build time. Clone with submodules:

```bash
git clone --recurse-submodules <repo-url>
# or, in an existing clone:
git submodule update --init --recursive
```

## Build & test

```bash
cmake --preset default          # configure (generates protobuf C++, fetches GoogleTest)
cmake --build build -j          # build the library, example, and tests
ctest --test-dir build -LE integration   # unit tests (no server needed)
```

### Integration tests

End-to-end tests run against a real server and are gated so the default run needs none:

```bash
temporal server start-dev &                                   # dev server on :7233
TEMPORAL_INTEGRATION=1 ctest --test-dir build -L integration  # run them
```

Without `TEMPORAL_INTEGRATION=1` they self-skip. CI (`.github/workflows/ci.yml`) runs both
suites, standing up a dev server for the integration pass.

The time-skipping test (`TimeSkippingFastForwardsTimers`) additionally needs the separate
[Temporal test server](https://github.com/temporalio/sdk-java/releases) binary and is gated on
`TEMPORAL_TEST_SERVER=host:port` (e.g. start `temporal-test-server 7244`, then export
`TEMPORAL_TEST_SERVER=localhost:7244`); it self-skips otherwise.

## Run the example end-to-end

```bash
temporal server start-dev                 # terminal 1: a local dev server on :7233
./build/examples/hello_world/hello_world  # terminal 2
# started workflow id=... run_id=...
# workflow result: Hello, Temporal!
```

You can inspect the run with `temporal workflow list` or the Web UI at http://localhost:8233.

## Documentation

The full documentation (English + Russian) is published at
**https://potatohd404.github.io/temporal-cpp-sdk/** and its source lives in
[`docs/`](docs/) (a [Docusaurus](https://docusaurus.io) site). Highlights:

- [Getting started](https://potatohd404.github.io/temporal-cpp-sdk/getting-started) ·
  [Tutorial](https://potatohd404.github.io/temporal-cpp-sdk/tutorial) ·
  [Core concepts](https://potatohd404.github.io/temporal-cpp-sdk/concepts)
- [Writing workflows](https://potatohd404.github.io/temporal-cpp-sdk/workflows/overview) ·
  [Client & worker](https://potatohd404.github.io/temporal-cpp-sdk/client-and-worker) ·
  [Testing](https://potatohd404.github.io/temporal-cpp-sdk/testing)
- [Architecture](https://potatohd404.github.io/temporal-cpp-sdk/architecture) ·
  [Parity matrix](https://potatohd404.github.io/temporal-cpp-sdk/parity)

## Project layout

```
include/temporal/   Public headers (client/ worker/ workflow/ activity/ converter/ common/ log/ testing/)
src/                Implementation; src/internal/ is the native engine (not installed)
cmake/              Protobuf/gRPC code generation
examples/           Runnable examples
tests/              GoogleTest unit + integration tests
third_party/        Vendored protos + reference SDKs (submodules)
docs/               Documentation site (Docusaurus, EN + RU) — deployed to GitHub Pages
```

## References

- [Temporal Go SDK](https://github.com/temporalio/sdk-go) — the native SDK this mirrors
- [Temporal Python SDK](https://github.com/temporalio/sdk-python) — a core-based SDK, used to
  understand the lang/engine boundary
- [temporalio/api](https://github.com/temporalio/api) — the protobuf API definitions
- [Temporal documentation](https://docs.temporal.io) — concepts that apply to every SDK

## License

MIT — see [LICENSE](LICENSE). Not affiliated with or endorsed by Temporal Technologies, Inc.
