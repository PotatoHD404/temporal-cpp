---
title: Отмена и прерывание
description: Отмените воркфлоу корректно или прервите его принудительно, наблюдайте отмену внутри воркфлоу и сворачивайте выполняющиеся таймеры, активности и дочерние воркфлоу.
---

# Отмена и прерывание

Есть два способа остановить выполняющийся воркфлоу со стороны клиента, и они не одно и то же:

- **`WorkflowHandle::Cancel()`** — *кооперативный / корректный.* Сервер записывает запрос на отмену;
  воркфлоу **наблюдает** его и сам решает, как отреагировать: выполнить компенсирующую активность, свернуть
  выполняющуюся работу, сохранить финальное состояние и вернуться. Ничего не навязывается.
- **`WorkflowHandle::Terminate(reason)`** — *принудительный / немедленный.* Сервер закрывает выполнение
  на месте. Воркфлоу **не получает шанса прибраться** — никакой дальнейший код не выполняется, выполняющиеся
  активности бросаются (их отмена выполняется по принципу best-effort), а причина записывается в событие
  закрытия.

```cpp
auto handle = client.GetHandle("order-123");
handle.Cancel();                      // просим воркфлоу свернуться
handle.Terminate("stuck; operator");  // убиваем его прямо сейчас, без уборки
```

Тянитесь к `Cancel()` всякий раз, когда воркфлоу держит ресурсы, которые нужно освободить, или имеет внешние
побочные эффекты, которые нужно скомпенсировать. Используйте `Terminate()` только когда воркфлоу заклинило
(например, недетерминированный баг, взаимоблокировка) и корректное завершение невозможно или не стоит ожидания.

В любом случае на стороне клиента закрытие проявляется как `WorkflowFailedError` от `Result<R>()` —
отменённый или прерванный воркфлоу **не** завершился успешно:

```cpp
try {
  handle.Result<std::string>();
} catch (const temporal::WorkflowFailedError& e) {
  // отменён, прерван, упал или истёк по таймауту
}
```

## Наблюдение отмены внутри воркфлоу

Запрос на отмену полезен, только если воркфлоу на него смотрит. Его проявляют два примитива.

### Опрос через `IsCancelled()`

`ctx.IsCancelled()` возвращает `true`, как только отмена была запрошена. Он дёшев и детерминирован —
опрашивайте его между шагами цикла:

```cpp
std::string Cancellable(temporal::workflow::Context& ctx) {
  auto signals = ctx.GetSignalChannel<std::string>("go");
  while (true) {
    if (ctx.IsCancelled()) {
      return "cancelled";   // сворачиваемся и завершаемся
    }
    if (signals.Receive() == "stop") {
      return "stopped";
    }
  }
}
```

### Ожидание через `AwaitCancellation()`

`ctx.AwaitCancellation()` возвращает `Future<void>`, который завершается, когда воркфлоу отменяют.
В отличие от опроса, это позволяет воркфлоу **блокироваться** на реальной работе и всё же проснуться в тот
же миг, как приходит отмена — добавьте его как случай (case) в [`Selector`](/workflows/composition) рядом
с работой:

```cpp
#include <temporal/workflow/selector.h>

std::string CancelAware(temporal::workflow::Context& ctx) {
  auto timer     = ctx.NewTimer(std::chrono::seconds(60));
  auto cancelled = ctx.AwaitCancellation();

  std::string out;
  temporal::workflow::Selector sel(ctx);
  sel.AddFuture(timer,     [&] { out = "timer-fired"; });
  sel.AddFuture(cancelled, [&] {
    timer.Cancel();        // сворачиваем выполняющуюся работу
    out = "cancelled";
  });
  sel.Select();            // просыпается на том, что случится первым
  return out;
}
```

Поскольку `AwaitCancellation()` отдаёт `Future<void>`, регистрируйте его **нешаблонной**
перегрузкой `AddFuture(future, handler)` — без `<void>` (как у таймера; см.
[селекторы](/workflows/composition)).

### Паттерн уборки-при-отмене

Корректный воркфлоу выполняет финальный компенсирующий шаг перед возвратом. Загвоздка: сама эта компенсация
задействует обычную активность, а воркфлоу находится в отменённом состоянии — поэтому выполняйте уборку
*после* наблюдения отмены, на выходе:

```cpp
std::string Booking(temporal::workflow::Context& ctx, std::string order) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(30);

  ctx.ExecuteActivity<void>(o, "Reserve", order).Get();

  // Ждём отмашки, но выходим при отмене.
  auto confirm   = ctx.GetSignalChannel<std::string>("confirm");
  auto cancelled = ctx.AwaitCancellation();

  bool aborted = false;
  temporal::workflow::Selector sel(ctx);
  sel.AddReceive<std::string>(confirm, [&](std::string) { /* продолжаем */ });
  sel.AddFuture(cancelled, [&] { aborted = true; });
  sel.Select();

  if (aborted) {
    ctx.ExecuteActivity<void>(o, "ReleaseReservation", order).Get();  // компенсируем
    return "released";
  }
  ctx.ExecuteActivity<void>(o, "Charge", order).Get();
  return "confirmed";
}
```

## Отмена выполняющихся операций

Отмена *воркфлоу* не отменяет автоматически *операции*, которые он запустил — распространение
явное. Вызовите `Future::Cancel()` у конкретного future, который хотите свернуть. Команда отмены
записывается в историю, поэтому реплеится детерминированно.

### Таймеры

`Cancel()` у future таймера разрешает его немедленно — 60-секундный таймер, который отменяют, возвращается
сразу же, а не дожидается истечения (история записывает `StartTimer` + `CancelTimer`):

```cpp
std::string TimerCancel(temporal::workflow::Context& ctx) {
  auto timer = ctx.NewTimer(std::chrono::seconds(60));
  timer.Cancel();
  timer.Get();          // возвращается немедленно, а не через 60 с
  return "cancelled";
}
```

### Активности

`Cancel()` у future активности испускает `RequestCancelActivityTask`. Выполняющаяся активность видит
запрос через свой **heartbeat** — поэтому наблюдать отмену может только активность с heartbeat. На стороне
активности вызывайте `RecordHeartbeat(...)` периодически и проверяйте `activity::Context::IsCancelled()`:

```cpp
// Активность: отправляет heartbeat, затем быстро возвращается, как только запрошена отмена.
std::string CancellableActivity(temporal::activity::Context& ctx, int) {
  for (int i = 0; i < 100; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ctx.RecordHeartbeat(i);          // отмена доставляется через heartbeat
    if (ctx.IsCancelled()) {
      return "cancelled";
    }
  }
  return "finished";
}

// Воркфлоу: при отмене отменяет активность и сообщает её результат.
std::string ActivityCancel(temporal::workflow::Context& ctx) {
  temporal::ActivityOptions o;
  o.start_to_close_timeout = std::chrono::seconds(60);
  o.heartbeat_timeout      = std::chrono::seconds(5);   // обязателен для доставки отмены

  auto act       = ctx.ExecuteActivity<std::string>(o, "CancellableActivity", 0);
  auto cancelled = ctx.AwaitCancellation();

  temporal::workflow::Selector sel(ctx);
  sel.AddFuture<std::string>(act, [](std::string) {});  // активность завершилась сама
  sel.AddFuture(cancelled, [&] { act.Cancel(); });      // отмена воркфлоу -> отменяем активность
  sel.Select();
  return act.Get();   // "cancelled", как только активность наблюдает запрос
}
```

:::note Активностям нужен heartbeat-таймаут
Активность, которая никогда не вызывает `RecordHeartbeat` — или у которой `ActivityOptions::heartbeat_timeout`
не задан — не может наблюдать отмену. Задавайте heartbeat-таймаут и отправляйте heartbeat из любой длительной
активности, которую вы намерены отменять.
:::

### Дочерние воркфлоу

`Cancel()` у future дочернего воркфлоу запрашивает отмену дочернего; дочерний наблюдает её, как
любой другой воркфлоу (`IsCancelled()` / `AwaitCancellation()`), и завершается на своих условиях:

```cpp
std::string CancelChild(temporal::workflow::Context& ctx) {
  temporal::ChildWorkflowOptions o;
  auto child = ctx.ExecuteChildWorkflow<std::string>(o, "CancelAware");
  ctx.Sleep(std::chrono::seconds(1));   // даём дочернему сначала стартовать
  child.Cancel();
  return child.Get();                   // результат дочернего, например "cancelled"
}
```

:::note Не запускайте и не отменяйте дочерний воркфлоу в одной задаче
Temporal запрещает запуск дочернего воркфлоу и запрос его отмены в пределах *одной*
задачи воркфлоу. Сначала вытолкните отмену в более позднюю задачу — например, коротким `ctx.Sleep(...)`,
как выше — иначе сервер отклонит команду.
:::

Чтобы управлять всё ещё выполняющимся дочерним при *закрытии* родителя (а не посреди запуска), задайте
`ChildWorkflowOptions::parent_close_policy` (`Terminate` / `Abandon` / `RequestCancel`) — см.
[дочерние воркфлоу](/workflows/composition).

## Отмена и сигнализация внешних воркфлоу

Воркфлоу может обратиться к **несвязанному** выполняющемуся воркфлоу по id — fire-and-forget, без handle:

```cpp
std::string Canceller(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.CancelExternalWorkflow(target_id);   // запрашиваем отмену другого воркфлоу
  ctx.Sleep(std::chrono::seconds(3));      // остаёмся в живых, чтобы запрос был доставлен
  return "done";
}

std::string Notifier(temporal::workflow::Context& ctx, std::string target_id) {
  ctx.SignalExternalWorkflow(target_id, "setName", std::string("World"));  // кодирует аргументы
  ctx.Sleep(std::chrono::seconds(3));
  return "done";
}
```

`CancelExternalWorkflow(workflow_id)` и `SignalExternalWorkflow(workflow_id, signal_name, args...)`
оба являются детерминированными командами. Они доставляются асинхронно, поэтому воркфлоу, который не делает
ничего, кроме отмены/сигнализации другого, должен ненадолго остаться в живых (как выше), чтобы запрос ушёл
до того, как он закроется. Те же вызовы нацеливаются на известный дочерний воркфлоу по его id — см.
[сигналы, запросы и обновления](/workflows/messaging).

:::note Нет структурированных областей отмены
Здесь **нет** API вложенности/областей (нет аналога `CancelChildContext` /
дерева отключённых контекстов из Go SDK). Распространение отмены **явное**: наблюдайте через `IsCancelled()` /
`AwaitCancellation()` и отменяйте `Future` каждой операции самостоятельно (таймер, активность или дочерний
воркфлоу). Это покрывает те же случаи без неявного графа областей — см.
[матрицу паритета](/parity).
:::
