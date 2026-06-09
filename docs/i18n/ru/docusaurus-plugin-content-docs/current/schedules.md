---
title: Расписания
description: Серверные повторяющиеся запуски воркфлоу — создание расписания по интервалу или cron, а затем его описание, обновление, приостановка/возобновление, ручной запуск, перечисление и удаление.
---

# Расписания

**Расписание** (Schedule) — это серверный объект, запускающий воркфлоу по повторяющейся спецификации.
В отличие от цикла в вашем собственном процессе, расписание живёт в кластере Temporal: оно
продолжает срабатывать при перезапусках клиента и воркера, и вы управляете им через стабильный
`schedule_id`, а не через handle к одному запуску. Каждое срабатывание запускает
сконфигурированный воркфлоу как независимое выполнение.

Расписаниями полностью управляют со стороны клиента — `temporal::client::Client`
предоставляет полный жизненный цикл. Спецификация и стартовое действие переносятся в
`temporal::ScheduleOptions`.

:::note
Этот SDK предоставляет намеренно минимальное подмножество API расписаний Temporal:
спецификацию по **интервалу** и/или **cron**-выражения плюс действие запуска воркфлоу. См.
[Ограничения](#limits), чтобы узнать, что предоставляется, а что нет.
:::

## Создание расписания {#creating}

Заполните `ScheduleOptions` и вызовите `CreateSchedule(schedule_id, options)`. Действие
всегда «запустить этот воркфлоу»; вы обязаны задать `workflow_type` и
`task_queue`. `workflow_id` необязателен и по умолчанию равен `<schedule id>-workflow`.

```cpp
struct ScheduleOptions {
  std::chrono::seconds interval{0};            // выполнять действие каждые `interval`
  std::vector<std::string> cron_expressions;   // календарные/cron-триггеры
  std::string workflow_type;                   // обязательно: запускаемый воркфлоу
  std::string task_queue;                      // обязательно: его очередь задач
  std::string workflow_id;                     // по умолчанию: "<schedule id>-workflow"
};
```

### Спецификация по интервалу {#interval}

Задайте `interval`, чтобы срабатывать с фиксированным периодом. Это поле имеет тип `std::chrono::seconds`,
поэтому подходит любая chrono-длительность, конвертируемая в секунды:

```cpp
#include <temporal/temporal.h>

temporal::ScheduleOptions opts;
opts.interval = std::chrono::hours(1);   // запускать раз в час
opts.workflow_type = "EchoWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("hourly-report", opts);
```

### Спецификация cron / календарь {#cron}

Задайте `cron_expressions`, чтобы срабатывать по календарю. Каждая строка — это **один** триггер:
стандартный cron из 5 полей (`minute hour day-of-month month day-of-week`), опционально
с **ведущим полем секунд** (что делает его 6-польным) и/или завершающим
`CRON_TZ=<zone>`. Укажите несколько выражений, чтобы срабатывать по любому из них.

```cpp
temporal::ScheduleOptions opts;
opts.cron_expressions = {"0 9 * * MON-FRI"};   // по будням в 09:00
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.CreateSchedule("daily-report", opts);
```

```cpp
// 6-польная форма (ведущие секунды) + таймзона: каждые 30 минут, в Europe/Berlin.
opts.cron_expressions = {"0 0,30 * * * * CRON_TZ=Europe/Berlin"};
```

`interval` и `cron_expressions` не являются взаимоисключающими — задайте оба, и
расписание будет срабатывать по интервалу **и** по каждому cron-триггеру. Задайте только одно (или, как
выше, несколько cron-строк), чтобы использовать эту спецификацию отдельно.

## Жизненный цикл {#lifecycle}

Все вызовы жизненного цикла принимают `schedule_id`, с которым вы создали расписание.
Соответствующие сигнатуры в `Client`:

```cpp
void                     CreateSchedule(const std::string& schedule_id, const ScheduleOptions& options);
bool                     DescribeSchedule(const std::string& schedule_id);
void                     UpdateSchedule(const std::string& schedule_id, const ScheduleOptions& options);
void                     TriggerSchedule(const std::string& schedule_id);
void                     PauseSchedule(const std::string& schedule_id, const std::string& note = "");
void                     UnpauseSchedule(const std::string& schedule_id, const std::string& note = "");
void                     DeleteSchedule(const std::string& schedule_id);
std::vector<std::string> ListSchedules();
```

### Описание {#describe}

`DescribeSchedule` возвращает, **существует** ли расписание — `true`, если присутствует,
`false`, если не найдено. Он бросает исключение только при ошибках, отличных от not-found, поэтому служит
заодно и проверкой существования:

```cpp
bool exists = client.DescribeSchedule("daily-report");
```

### Обновление {#update}

`UpdateSchedule` целиком заменяет спецификацию и действие расписания свежим
`ScheduleOptions` — частичного патча нет. Соберите опции, которые расписание должно
иметь с этого момента, и передайте их:

```cpp
temporal::ScheduleOptions opts;
opts.interval = std::chrono::hours(2);   // было ежечасно; теперь каждые два часа
opts.workflow_type = "ReportWorkflow";
opts.task_queue = "reports";

client.UpdateSchedule("daily-report", opts);
```

### Приостановка / возобновление {#pause}

Приостановка прекращает срабатывание расписания, сохраняя его определение; возобновление возвращает
его к работе. Оба принимают необязательную `note`, записываемую в расписание. Приостановленное расписание по-прежнему
существует и по-прежнему доступно для описания.

```cpp
client.PauseSchedule("daily-report", "freezing reports during migration");
// ... позже ...
client.UnpauseSchedule("daily-report", "migration done");
```

### Ручной запуск (запустить сейчас) {#trigger}

`TriggerSchedule` запускает действие **немедленно**, независимо от спецификации и
даже когда расписание приостановлено. Используйте его, чтобы запустить запланированный воркфлоу по требованию
без ожидания следующего срабатывания.

```cpp
client.TriggerSchedule("daily-report");
```

### Перечисление {#list}

`ListSchedules` возвращает идентификаторы всех расписаний в namespace, постранично перебирая результаты
внутренне. Перечисление **согласовано в конечном счёте** — только что созданное расписание может не
появиться сразу:

```cpp
std::vector<std::string> ids = client.ListSchedules();
for (const std::string& id : ids) {
  // ...
}
```

### Удаление {#delete}

`DeleteSchedule` удаляет расписание. Это не затрагивает запуски воркфлоу, которые расписание
уже запустило, — только будущие срабатывания.

```cpp
client.DeleteSchedule("daily-report");
```

## Сквозной пример {#example}

Создайте cron-расписание, подтвердите его существование, запустите его один раз по требованию, приостановите его и,
наконец, удалите:

```cpp
#include <temporal/temporal.h>

void ManageDailyReport(temporal::client::Client& client) {
  const std::string schedule_id = "daily-report";

  // 1. Создание — по будням утром в 09:00.
  temporal::ScheduleOptions opts;
  opts.cron_expressions = {"0 9 * * MON-FRI"};
  opts.workflow_type = "ReportWorkflow";
  opts.task_queue = "reports";
  client.CreateSchedule(schedule_id, opts);

  // 2. Описание — подтверждаем, что сервер его зарегистрировал.
  if (!client.DescribeSchedule(schedule_id)) {
    throw std::runtime_error("schedule was not created");
  }

  // 3. Ручной запуск — запускаем его один раз прямо сейчас, не дожидаясь 09:00.
  client.TriggerSchedule(schedule_id);

  // 4. Приостановка — останавливаем будущие срабатывания (ручной запуск выше всё равно отработал).
  client.PauseSchedule(schedule_id, "paused after a manual run");

  // 5. Удаление — сносим его (уже запущенные выполнения не затрагиваются).
  client.DeleteSchedule(schedule_id);
}
```

## Ограничения {#limits}

Поверхность расписаний здесь намеренно невелика. Проверено против текущего API:

- **Действие — только запуск воркфлоу.** Каждое расписание запускает воркфлоу, названный
  `ScheduleOptions::workflow_type`, в `task_queue`; других типов действий не существует.
- **Нет аргументов воркфлоу в действии.** `ScheduleOptions` несёт тип воркфлоу,
  очередь задач и (необязательный) id воркфлоу — поля для стартовых
  аргументов, memo или search attributes для запланированного запуска нет.
- **`DescribeSchedule` возвращает только факт существования.** Он выдаёт `bool`, а не богатый
  объект описания — структуры schedule-info / next-run-time / recent-actions
  в этом SDK нет.
- **Спецификация — интервал и/или cron.** Предоставляются только `interval` и `cron_expressions`.
  Структурированные календарные спецификации (явные списки дней/месяцев, диапазоны пропусков,
  jitter) и **политика перекрытия** (overlap policy) для одновременных запусков **не**
  предоставляются.
- **Нет приостановки при создании.** Расписание стартует активным; приостановите его явно с помощью
  `PauseSchedule`, если вам нужно, чтобы оно стартовало приостановленным.
- **`ListSchedules` согласован в конечном счёте.** Только что созданное расписание может появиться
  с задержкой; опрашивайте его, если нужно увидеть его сразу после создания.
