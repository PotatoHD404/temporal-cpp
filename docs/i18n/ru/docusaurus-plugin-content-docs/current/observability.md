---
title: Наблюдаемость
description: Эмитируйте метрики SDK через MetricsHandler, направляйте структурированные логи через подключаемый Logger и распространяйте распределённые трассировки с помощью фреймворка интерсепторов — адаптировано к API temporal-cpp-sdk.
---

# Наблюдаемость

Наблюдаемость воркера `temporal-cpp-sdk` опирается на три столпа, каждый из которых подключаемый, так что вы можете направить его
в любой бэкенд, который у вас уже работает:

- **[Метрики](#metrics)** — sink `MetricsHandler`, который вы задаёте в `WorkerOptions`; SDK эмитирует набор
  серий по поллерам, задержкам задач, слотам и sticky-кэшу через него.
- **[Логирование](#logging)** — `temporal::log::Logger`, который вы задаёте в `ClientOptions`; SDK никогда не пишет
  в `stdout`/`stderr` напрямую. Внутри воркфлоу или активности доберитесь до того же логгера через
  контекст.
- **[Трассировка](#tracing)** — фреймворк на основе интерсепторов с принципом bring-your-own `Tracer`; спан
  начинается вокруг каждого воркфлоу/активности, а его контекст распространяется через границу с помощью
  заголовка Temporal.

:::note
Здесь нет встроенного эндпоинта Prometheus для scrape, нет экспортёра OpenTelemetry и нет конвейера push-метрик
— каждый столп является интерфейсом, который вы реализуете под свой собственный стек. SDK гарантирует
лишь то, что направляет свои сигналы через эти интерфейсы; экспорт — за вами. Более широкую картину см. в
[матрице совместимости](/parity).
:::

---

## Метрики {#metrics}

Воркер сообщает свою внутреннюю телеметрию через интерфейс `temporal::MetricsHandler`
(`<temporal/common/options.h>`). Реализуйте его, чтобы пересылать в Prometheus, StatsD, OpenTelemetry или любой
бэкенд, затем задайте его в `WorkerOptions::metrics_handler`. Если оставить handler равным `nullptr` (значение
по умолчанию), воркер ничего не эмитирует — накладных расходов нет.

### Интерфейс

```cpp
namespace temporal {

class MetricsHandler {
 public:
  using Tags = std::map<std::string, std::string>;

  virtual void Counter(const std::string& name, std::int64_t value, const Tags& tags) = 0;
  virtual void Gauge(const std::string& name, double value, const Tags& tags) = 0;
  virtual void Timer(const std::string& name, std::chrono::nanoseconds value, const Tags& tags) = 0;
};

}  // namespace temporal
```

Три примитива: `Counter` для монотонных подсчётов событий, `Gauge` для значений уровня на момент времени и `Timer`
для длительностей (доставляются как `std::chrono::nanoseconds` — конвертируйте в любую единицу, которую хочет ваш бэкенд).
Каждый вызов несёт map `Tags`; сопоставьте каждый тег с концепцией label/dimension вашего бэкенда.

:::note
Handler вызывается из потоков-поллеров воркера, конкурентно. Ваша реализация обязана быть
**потокобезопасной**. Счётчики в примере ниже используют `std::atomic`; реальный адаптер обычно пересылает
напрямую потокобезопасному клиенту.
:::

### Установка его на воркере

```cpp
#include <temporal/common/options.h>
#include <temporal/worker/worker.h>

auto metrics = std::make_shared<MyMetricsHandler>();

temporal::WorkerOptions opts;
opts.metrics_handler = metrics;   // nullptr (по умолчанию) отключает эмиссию

temporal::worker::Worker worker(client, "my-task-queue", opts);
```

### Метрики, которые эмитирует SDK

Воркер эмитирует следующие серии. Имена — это стабильные идентификаторы; теги перечислены там, где они
прикреплены (серии без перечисленных тегов эмитируются с пустой map тегов).

#### Жизненный цикл поллера

| Метрика | Тип | Теги | Значение |
|---|---|---|---|
| `temporal_poller_start` | Counter | `task_queue`, `poller_type` | Поток-поллер запустился. `poller_type` — это `workflow`, `sticky`, `activity`, `session` или `nexus`. |
| `temporal_pollers_in_flight` | Gauge | `task_queue` | Активные в данный момент поллеры для этого вида цикла (живой счётчик в рамках [автомасштабирования поллеров](/production#worker-options)). |
| `temporal_workflow_poll_success` / `temporal_activity_poll_success` / `temporal_nexus_poll_success` | Counter | — | Long-poll вернул задачу. |
| `temporal_workflow_poll_timeout` / `temporal_activity_poll_timeout` / `temporal_nexus_poll_timeout` | Counter | — | Long-poll вернул пусто (нет задачи до истечения серверного poll-таймаута). |

#### Таймеры задержек задач

Воркер измеряет обработку задачи по фазам — schedule-to-start, выполнение и (для задач воркфлоу и
активностей) сквозную:

| Метрика | Тип | Теги | Значение |
|---|---|---|---|
| `temporal_workflow_task_schedule_to_start_latency` | Timer | `task_queue` | Время, которое задача воркфлоу прождала локально (concurrency-gate / rate-limiter) между **получением** и **началом выполнения**. |
| `temporal_workflow_task_execution_latency` | Timer | — | Wall-clock-время, которое handler потратил на выполнение задачи воркфлоу. |
| `temporal_workflow_task_end_to_end_latency` | Timer | `task_queue` | Промежуток от получения до завершения всей задачи (schedule-to-start **плюс** выполнение). |
| `temporal_activity_task_schedule_to_start_latency` | Timer | `task_queue` | Как выше, для задачи активности. |
| `temporal_activity_task_execution_latency` | Timer | — | Время, в течение которого выполнялась функция активности. |
| `temporal_activity_task_end_to_end_latency` | Timer | `task_queue` | От получения до завершения для задачи активности. |
| `temporal_nexus_task_schedule_to_start_latency` / `temporal_nexus_task_execution_latency` | Timer | `task_queue` / — | Те же две фазы для задачи Nexus. |

:::note
Эти таймеры schedule-to-start измеряют **локальное** ожидание на данном воркере (gate + rate-limiter), а не
серверный schedule-to-start, о котором сообщают Go/Java SDK. Они являются прокси для вопроса «не накапливается ли
очередь на этом воркере?», а не для сквозной задержки очереди на сервере.
:::

#### Счётчики исходов задач

| Метрика | Тип | Теги | Значение |
|---|---|---|---|
| `temporal_workflow_task_handled` / `temporal_activity_task_handled` / `temporal_nexus_task_handled` | Counter | — | Задача завершена (её handler вернулся без выброса исключения). |
| `temporal_workflow_task_failed` / `temporal_activity_task_failed` / `temporal_nexus_task_failed` | Counter | `task_queue` | Цикл poll/dispatch поймал исключение при обработке задачи. |

#### Слоты и in-flight gauge

| Метрика | Тип | Теги | Значение |
|---|---|---|---|
| `temporal_workflow_tasks_in_flight` / `temporal_activity_tasks_in_flight` | Gauge | — | Задачи, выполняющиеся в данный момент (удерживаемые concurrency-gate). |
| `temporal_worker_task_slots_available` | Gauge | `task_queue`, `worker_type` | Свободные слоты выполнения = сконфигурированный лимит − in-flight. **Эмитируется только когда задан соответствующий лимит** (`max_concurrent_workflow_task_executions` / `max_concurrent_activity_executions`); при неограниченном лимите нет счётчика слотов для отчёта. `worker_type` — это `workflow`, `activity` или `session`. |

#### Эффективность sticky-кэша

Эмитируется после каждой обработанной задачи воркфлоу (об устройстве см. [Sticky-кэш](/production#sticky-cache)):

| Метрика | Тип | Теги | Значение |
|---|---|---|---|
| `temporal_sticky_cache_hit` | Counter | `task_queue` | Задача воркфлоу обслужена из резидентной корутины — без реплея. Эмитируется как дельта. |
| `temporal_sticky_cache_miss` | Counter | `task_queue` | Задача воркфлоу, потребовавшая реплея полной истории (холодный старт или после вытеснения). Эмитируется как дельта. |
| `temporal_sticky_cache_total_hits` / `temporal_sticky_cache_total_misses` | Gauge | `task_queue` | Кумулятивные итоги попаданий/промахов с момента запуска воркера (те же числа, что возвращают `worker.cache_hits()` / `worker.replays()`). |
| `temporal_sticky_cache_size` | Gauge | `task_queue` | Сконфигурированная ёмкость кэша — **эмитируется только когда `max_cached_workflows > 0`** (ни один аксессор не выдаёт живой счётчик резидентных корутин, поэтому сообщается граница). |

#### Обнаружение deadlock

| Метрика | Тип | Теги | Значение |
|---|---|---|---|
| `temporal_workflow_task_deadlock` | Counter | — | Задача воркфлоу превысила `deadlock_detection_timeout` и была прервана. **Срабатывающий счётчик — это дефект кода**, а не сигнал для тюнинга — см. [Обнаружение deadlock](/production#deadlock-detection). |

### Пользовательский MetricsHandler

Минимальный handler, пересылающий счётчики, gauge и таймеры вашему собственному клиенту метрик. Полная
map `Tags` пробрасывается насквозь, чтобы ваш бэкенд мог прикреплять labels:

```cpp
#include <temporal/common/options.h>

#include "my_metrics_client.h"  // ваш клиент StatsD / Prometheus / OTel

class MyMetricsHandler : public temporal::MetricsHandler {
 public:
  explicit MyMetricsHandler(std::shared_ptr<my_metrics::Client> client)
      : client_(std::move(client)) {}

  void Counter(const std::string& name, std::int64_t value, const Tags& tags) override {
    client_->IncrementBy(name, value, tags);
  }

  void Gauge(const std::string& name, double value, const Tags& tags) override {
    client_->SetGauge(name, value, tags);
  }

  void Timer(const std::string& name, std::chrono::nanoseconds value, const Tags& tags) override {
    // Конвертируем в предпочитаемую единицу вашего бэкенда; здесь — миллисекунды.
    const double ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(value).count();
    client_->Timing(name, ms, tags);
  }

 private:
  std::shared_ptr<my_metrics::Client> client_;  // предполагается потокобезопасным
};
```

Если вам нужны лишь несколько серий, фильтруйте по `name` внутри каждого метода — воркер вызывает handler
для каждой метрики, и игнорирование тех, что вас не интересуют, бесплатно.

### Прямой опрос счётчиков кэша

Два самых важных показателя здоровья воркера также доступны как обычные аксессоры, независимо от
metrics-handler, так что вы можете сэмплировать их по своему собственному расписанию:

```cpp
long hits   = worker.cache_hits();  // == temporal_sticky_cache_total_hits
long miss   = worker.replays();     // == temporal_sticky_cache_total_misses
double rate = (double)hits / std::max(1L, hits + miss);
```

О том, как интерпретировать hit rate, см. [Наблюдение за эффективностью кэша](/production#sticky-cache).

---

## Логирование {#logging}

В SDK действует строгое правило: **ничего не пишется в `stdout`/`stderr` напрямую** — никакого `printf`, никакого
`std::cout`, никакого голого логирования нигде. Каждое внутреннее сообщение проходит через
`temporal::log::Logger` (`<temporal/log/logger.h>`), который вы предоставляете, так что воркер, движок реплея
и слой gRPC — все используют один структурированный поток, которым вы управляете.

### Интерфейс

```cpp
namespace temporal::log {

enum class Level : std::uint8_t { Debug, Info, Warn, Error };

// Структурированное поле — ключ/значение, оба строки.
struct Field {
  std::string key;
  std::string value;
};

// Вспомогательный конструктор, используемый в каждом месте вызова: log::F("key", "value").
inline Field F(std::string key, std::string value);

class Logger {
 public:
  // Единственный метод, который вы реализуете.
  virtual void Log(Level level, std::string_view message, const std::vector<Field>& fields) = 0;

  // Вспомогательные обёртки (невиртуальные), вызывающие Log() на фиксированном уровне.
  void Debug(std::string_view m, const std::vector<Field>& f = {});
  void Info(std::string_view m, const std::vector<Field>& f = {});
  void Warn(std::string_view m, const std::vector<Field>& f = {});
  void Error(std::string_view m, const std::vector<Field>& f = {});
};

// Значение по умолчанию на уровне процесса: одна структурированная строка на запись в stderr.
std::shared_ptr<Logger> DefaultLogger();

}  // namespace temporal::log
```

### Подключение вашего логгера

Задайте его один раз в `ClientOptions`; воркер, созданный из этого клиента, наследует тот же логгер:

```cpp
#include <temporal/log/logger.h>

class MyLogger : public temporal::log::Logger {
 public:
  void Log(temporal::log::Level level, std::string_view message,
           const std::vector<temporal::log::Field>& fields) override {
    // Направляем в slog / spdlog / absl::log / любой структурированный sink.
    auto rec = my_log_library::NewRecord(to_severity(level), message);
    for (const auto& f : fields) {
      rec.AddField(f.key, f.value);
    }
    rec.Flush();
  }
};

temporal::ClientOptions opts;
opts.logger = std::make_shared<MyLogger>();
auto client = temporal::client::Client::Connect(opts);
```

Если вы не задаёте логгер, используется `temporal::log::DefaultLogger()` — одна структурированная строка на запись в
`stderr`, что приемлемо для разработки.

SDK использует `Debug` для внутренних событий жизненного цикла (запуск поллеров, диспетчеризация задач, попадания/промахи кэша),
`Info` — для значимых изменений состояния и `Warn`/`Error` — для неожиданных ситуаций. Подавляйте `Debug` в
продакшене, если только вы не диагностируете что-то конкретное.

### Логирование из воркфлоу

Внутри воркфлоу получайте общий логгер из контекста — не захватывайте глобальный:

```cpp
std::string GreetWorkflow(temporal::workflow::Context& ctx, std::string name) {
  temporal::ActivityOptions opts;
  opts.start_to_close_timeout = std::chrono::seconds(10);
  std::string greeting = ctx.ExecuteActivity<std::string>(opts, "ComposeGreeting", name).Get();

  ctx.GetLogger().Info("composed greeting", {temporal::log::F("greeting", greeting)});
  return greeting;
}
```

`ctx.GetLogger()` возвращает ссылку на тот же `log::Logger`, который использует воркер, так что логи воркфлоу попадают
в ваш единый поток.

:::warning Реплей и дублирующиеся логи
Логгер, возвращаемый `ctx.GetLogger()`, **не заглушается автоматически во время реплея**. Задача
воркфлоу повторно прогоняет тело вашего воркфлоу с начала при холодном старте или после вытеснения из кэша, так что любая
незащищённая строка лога эмитируется снова на каждом реплее. Защищайте логи, которые нужны лишь однократно, с помощью
`ctx.IsReplaying()`:

```cpp
if (!ctx.IsReplaying()) {
  ctx.GetLogger().Info("charged customer", {temporal::log::F("order_id", id)});
}
```

Это тот же флаг `IsReplaying()`, который интерсепторы используют, чтобы избежать двойного подсчёта (см. идиому теста
трассировки ниже). В коде воркфлоу всегда обращайтесь к `ctx.GetLogger()`, а не к логгеру вашего приложения,
и ставьте на строки логов, видимые пользователю, условие `!ctx.IsReplaying()`.
:::

### Логирование из активности

Активность выполняется в реальном времени без реплея, поэтому проблемы дублирующихся логов неприменимы. `Context`
активности **не** предоставляет аксессор `GetLogger()` — используйте собственный логгер вашего приложения напрямую и
прикрепляйте поля корреляции из `ctx.GetInfo()`:

```cpp
std::string ChargeCard(temporal::activity::Context& ctx, ChargeRequest req) {
  const auto& info = ctx.GetInfo();
  my_app::log().Info("charging card",
                     {{"workflow_id", info.workflow_id},
                      {"activity_id", info.activity_id},
                      {"attempt", std::to_string(info.attempt)}});
  // ... реальная работа, реальный I/O ...
  return "charged";
}
```

`ActivityInfo` несёт `workflow_id`, `run_id`, `activity_id`, `activity_type`, `task_queue`,
`attempt` и распространяемые `headers` — всё, что нужно для корреляции строки лога активности обратно с
её воркфлоу.

---

## Трассировка {#tracing}

Распределённая трассировка построена на **фреймворке интерсепторов** (`<temporal/interceptor/interceptor.h>`)
плюс интерсептор трассировки и **bring-your-own** `Tracer` (`<temporal/interceptor/tracing.h>`).

:::warning Экспортёр не входит в комплект
SDK поставляет *обвязку* трассировки — жизненный цикл спана вокруг выполнения воркфлоу/активности и
распространение контекста через границу — но **никакого экспортёра OpenTelemetry, OpenTracing или Jaeger**.
Вы реализуете абстрактные интерфейсы `Tracer`/`Span`, делая мост к вашему бэкенду трассировки. Единственные
реализации `Tracer`, которые предоставляет SDK, — это `NoopTracer` (ничего не делает) и `InMemoryTracer`
(только для тестов, записывает спаны в процессе). См. [матрицу совместимости](/parity).
:::

### Как трасса распространяется воркфлоу → активность

Фреймворк интерсепторов встроен в пути вызовов воркера и клиента. Встроенный
`TracingInterceptor`:

1. **Inbound** — когда выполнение воркфлоу или активности стартует, читает входящий заголовок Temporal,
   просит `Tracer` `Extract` контекст родительского спана из него и стартует спан как дочерний этого родителя
   (корневой спан, если родителя нет).
2. **Outbound** — когда воркфлоу планирует активность / дочерний воркфлоу / сигнал (или клиент
   запускает/сигналит воркфлоу), он просит `Tracer` `Inject` контекст текущего спана в плоскую
   `map<string,string>`, сериализует её в единственный `Payload` и записывает её в заголовок Temporal
   **header** под одним ключом (по умолчанию `"_tracer-data"`).

Поскольку контекст едет на тех же map заголовков, которые SDK уже распространяет
(`StartWorkflowOptions::headers` → `WorkflowInfo::headers` → `ActivityInfo::headers`), inbound-сторона активности
извлекает родителя, и два спана разделяют одну трассу. Формат на проводе — это единственное
JSON-кодированное значение заголовка, по духу совпадающее с другими SDK Temporal, так что межъязыковые трассы могут связываться.

### Включение трассировки на воркере

Сконструируйте `TracingInterceptor` поверх вашего `Tracer` и добавьте его в `WorkerOptions::interceptors`:

```cpp
#include <temporal/interceptor/tracing.h>

auto tracer  = std::make_shared<MyTracer>();                  // адаптер вашего бэкенда
auto tracing = std::make_shared<temporal::interceptor::TracingInterceptor>(tracer.get());

temporal::WorkerOptions wo;
wo.interceptors.push_back(tracing);

temporal::worker::Worker worker(client, "my-task-queue", wo);
```

Чтобы также стартовать спан на стороне *клиентского* вызова (так чтобы у спана воркфлоу был родитель), добавьте тот же
интерсептор в `ClientOptions::interceptors`:

```cpp
temporal::ClientOptions opts;
opts.interceptors.push_back(tracing);
auto client = temporal::client::Client::Connect(opts);
```

:::note
`Tracer` удерживается `TracingInterceptor`-ом без владения (он принимает сырой `Tracer*`). Держите
`Tracer` живым как минимум столько же, сколько живёт использующий его воркер/клиент — `std::shared_ptr` в
сниппетах выше делает именно это.
:::

### Интерфейсы Tracer / Span

Реальный адаптер бэкенда реализует `Tracer`; `StartSpan` возвращает `Span`, а `Inject`/`Extract` переносят
контекст через провод как плоскую string-map:

```cpp
namespace temporal::interceptor {

class Span {
 public:
  virtual void SetTag(const std::string& key, const std::string& value) = 0;
  virtual void End(bool error = false) = 0;          // error => трассируемая операция завершилась с ошибкой
  virtual const SpanContext& context() const = 0;     // родитель для дочернего / источник для Inject
};

class Tracer {
 public:
  virtual std::unique_ptr<Span> StartSpan(const StartSpanOptions& options) = 0;  // никогда не null
  virtual std::map<std::string, std::string> Inject(const Span& span) const = 0;  // empty => нет заголовка
  virtual std::optional<SpanContext> Extract(
      const std::map<std::string, std::string>& data) const = 0;  // nullopt => нет родителя
};

}  // namespace temporal::interceptor
```

`StartSpanOptions` несёт высокоуровневую `operation` (например, `"RunWorkflow"`, `"RunActivity"`),
конкретное `name` воркфлоу/активности, необязательный `parent` `SpanContext*` (извлечённый из входящего
заголовка) и map `tags`. Предоставляется `NoopTracer`, так что местам вызова не нужны проверки на null, когда трассировка
«включена», но не сконфигурирована.

### Проверка распространения с помощью in-memory tracer

Для тестов и локальных прогонов `InMemoryTracer` записывает каждый спан в процессе и прогоняет
идентификаторы трассы/спана через map распространения туда-обратно, так что вы можете утверждать, что спан активности унаследовал
трассу спана воркфлоу, не поднимая бэкенд:

```cpp
auto tracer  = std::make_shared<temporal::interceptor::InMemoryTracer>();
auto tracing = std::make_shared<temporal::interceptor::TracingInterceptor>(tracer.get());

temporal::WorkerOptions wo;
wo.interceptors.push_back(tracing);
temporal::worker::Worker worker(client, task_queue, wo);
worker.RegisterWorkflow("EchoWorkflow", EchoWorkflow);
worker.RegisterActivity("Echo", EchoActivity);
worker.Start();

// ... запускаем воркфлоу ...

worker.Stop();  // присоединяем потоки воркера перед чтением записанных спанов

const auto& recs = tracer->records();   // каждый спан, в порядке старта
// recs содержит как минимум спан воркфлоу и спан активности, разделяющие один trace_id.
```

Каждый `InMemoryTracer::Record` предоставляет `operation`, `name`, `trace_id`, `span_id`, `parent_span_id`
(пустой для корневого), `tags` и флаги `ended`/`error` — достаточно, чтобы утверждать топологию трассы в юнит-
тесте.

### Интерсепторы помимо трассировки

`TracingInterceptor` — это одна из реализаций общей поверхности интерсепторов. Те же
списки `WorkerOptions::interceptors` / `ClientOptions::interceptors` принимают любой
`temporal::interceptor::Interceptor`, позволяя вам оборачивать вызовы workflow-inbound, activity-inbound,
workflow-outbound и client-outbound для ваших собственных сквозных задач (контекст аутентификации,
пользовательские метрики, структурированный аудит). Унаследуйтесь от соответствующего `…InterceptorBase` и переопределите только
нужный метод; например, интерсептор workflow-inbound может ставить побочные эффекты под условие
`ctx.IsReplaying()`, чтобы выполнять их ровно один раз:

```cpp
temporal::Payloads ExecuteWorkflow(temporal::workflow::Context& ctx,
                                   temporal::interceptor::ExecuteWorkflowInput& in,
                                   const temporal::interceptor::Header& header) override {
  if (!ctx.IsReplaying()) {
    ++live_executions_;  // подсчитывается один раз за реальный прогон, никогда на реплее
  }
  return next_->ExecuteWorkflow(ctx, in, header);
}
```

---

## См. также

- **[Запуск в продакшене](/production)** — тюнинг sticky-кэша, счётчик `temporal_workflow_task_deadlock`
  и обнаружение deadlock, выбор размеров воркера и честный список того, что ещё не готово
  для продакшена.
- **[Тестирование](/testing)** — реплей записанных историй, тестовое окружение воркфлоу и идиомы
  юнит-тестов (включая in-memory tracer выше).
- **[Возможности и совместимость](/parity)** — какие именно возможности наблюдаемости реализованы, а какие
  заглушены.
