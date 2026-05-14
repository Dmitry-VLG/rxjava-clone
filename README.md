# RxJava Clone

Учебная реализация упрощённой RxJava-подобной библиотеки на Java с поддержкой реактивной модели подписки,
операторов преобразования, асинхронного выполнения и управления потоками.
Реализация ориентирована на демонстрацию архитектурных принципов реактивных библиотек,
а не на полное повторение production-возможностей настоящей RxJava.

---

## 1. Цель

Цель — реализовать мини-библиотеку реактивных потоков со следующими возможностями:

- создание источника событий через `Observable.create(...)`;
- подписка через `subscribe(...)`;
- получение сигналов `onNext`, `onError`, `onComplete`;
- преобразование потока через `map`, `filter`, `flatMap`;
- управление потоками выполнения через `subscribeOn` и `observeOn`;
- отмена подписки через `Disposable`;
- обработка ошибок;
- покрытие unit-тестами.

---

## 2. Реализация

### Базовые компоненты

- `Observer<T>`
- `Observable<T>`
- `Observable.create(...)`
- `Disposable`
- `Emitter<T>`

### Операторы

- `map(Function)`
- `filter(Predicate)`
- `flatMap(Function<T, Observable<R>>)`

### Scheduler API

- `Scheduler`
- `IOThreadScheduler`
- `ComputationScheduler`
- `SingleThreadScheduler`
- `Schedulers.io()`
- `Schedulers.computation()`
- `Schedulers.single()`
- `Schedulers.shutdown()`
- `Schedulers.reset()`

### Управление потоками

- `subscribeOn(...)`
- `observeOn(...)`

### Дополнительно

- сериализация downstream-сигналов в конкурентных сценариях;
- корректная обработка terminal-сигналов;
- расширенное тестовое покрытие многопоточного поведения.

---

## 3. Соответствие требованиям задания

| Требование                               | Реализация                                                                | Статус    |
|------------------------------------------|---------------------------------------------------------------------------|-----------|
| `Observer` с `onNext/onError/onComplete` | `ru.rxclone.core.Observer`                                                | Выполнено |
| `Observable` с `subscribe(...)`          | `ru.rxclone.core.Observable`                                              | Выполнено |
| `Observable.create(...)`                 | `Observable.create(...)` + `ObservableCreate`                             | Выполнено |
| `map(...)`                               | `ObservableMap`                                                           | Выполнено |
| `filter(...)`                            | `ObservableFilter`                                                        | Выполнено |
| `Scheduler.execute(...)`                 | `ru.rxclone.core.Scheduler`                                               | Выполнено |
| `IOThreadScheduler`                      | `ru.rxclone.schedulers.IOThreadScheduler`                                 | Выполнено |
| `ComputationScheduler`                   | `ru.rxclone.schedulers.ComputationScheduler`                              | Выполнено |
| `SingleThreadScheduler`                  | `ru.rxclone.schedulers.SingleThreadScheduler`                             | Выполнено |
| `subscribeOn(...)`                       | `ObservableSubscribeOn`                                                   | Выполнено |
| `observeOn(...)`                         | `ObservableObserveOn`                                                     | Выполнено |
| `flatMap(...)`                           | `ObservableFlatMap`                                                       | Выполнено |
| `Disposable`                             | `Disposable`, `BooleanDisposable`, `CompositeDisposable`, `CreateEmitter` | Выполнено |
| Обработка ошибок через `onError`         | `CreateEmitter`, операторы, сериализация terminal-сигналов                | Выполнено |
| Unit-тесты                               | `src/test/java/ru/rxclone`                                                | Выполнено |

---

## 4. Архитектура

### Структура

```text 
ru.rxclone
├── core
│ ├── Disposable
│ ├── Emitter
│ ├── Observable
│ ├── ObservableOnSubscribe
│ ├── Observer
│ └── Scheduler
├── disposables
│ ├── BooleanDisposable
│ └── CompositeDisposable
├── internal
│ ├── CreateEmitter
│ └── SerializedObserver
├── observables
│ ├── ObservableCreate
│ ├── ObservableMap
│ ├── ObservableFilter
│ ├── ObservableFlatMap
│ ├── ObservableSubscribeOn
│ └── ObservableObserveOn
├── schedulers
│ ├── IOThreadScheduler
│ ├── ComputationScheduler
│ ├── SingleThreadScheduler
│ └── Schedulers
└── demo
└── DemoMain
```
---

## 5. Архитектурная идея

`Observable<T>` — это абстракция источника событий.
Каждый оператор (map, filter, flatMap, subscribeOn, observeOn) не изменяет исходный поток, а создаёт новый Observable,
который оборачивает предыдущий. За счёт этого формируется композиция операторов и строится цепочка обработки.

### Пример:

```
Observable.<Integer>create(...)
    .map(...)
    .filter(...)
    .flatMap(...)
    .subscribeOn(...)
    .observeOn(...)
    .subscribe(...);
```

Выполнение начинается в момент вызова subscribe(...).

---

## 6. Схема прохождения сигналов

### Общая схема

```
Source (Observable.create)
|
v
ObservableCreate
|
v
Operator: map
|
v
Operator: filter
|
v
Operator: flatMap
|
v
subscribeOn / observeOn
|
v
Observer.onNext(...)
Observer.onError(...)
Observer.onComplete(...)
```

### Схема подписки

```
subscribe(observer)
    ->
subscribeActual(observer)
    ->
source/operator подписывает downstream observer
    ->
source начинает эмитить сигналы
    ->
downstream получает onNext/onError/onComplete
```

---

## 7. Описание ключевых компонентов

`Observer<T> `

### Подписчик, который получает сигналы потока:

```
onNext(T item) — новое значение;
onError(Throwable t) — ошибка;
onComplete() — успешное завершение потока.
Observable<T>
```

### Базовый абстрактный класс библиотеки, который предоставляет:

```
создание потока через create(...);
подписку через subscribe(...);
цепочку операторов.
Emitter<T>
```

### Внутренний интерфейс, через который источник в Observable.create(...) отправляет сигналы downstream-подписчику.

`Disposable`

### Интерфейс отмены подписки:

```
dispose() — отменить подписку;
isDisposed() — проверить статус.
CompositeDisposable
```

### Служебный класс для управления несколькими Disposable одновременно. Особенно важен в flatMap, где один внешний поток

### Может создать несколько внутренних подписок.

`CreateEmitter<T>`

`Основной emitter для Observable.create(...).`

### Отвечает за:

- доставку сигналов downstream;
- блокировку сигналов после terminal-state;
- перевод исключений в onError(...).
- SerializedObserver<T>

### Внутренний компонент, обеспечивающий сериализацию сигналов downstream в конкурентных сценариях.

### Его задача:

- не допускать одновременных вызовов downstream onNext(...);
- ставить сигналы в очередь;
- доставлять их последовательно;
- гарантировать, что terminal-сигнал остаётся terminal.
-

## 8. Операторы

### 8.1 `map(Function mapper)`

Преобразует каждый входной элемент в новый.

# Пример:

```Observable.<Integer>create(emitter -> {
    emitter.onNext(1);
    emitter.onNext(2);
    emitter.onComplete();
}).map(x -> x * 10);
```

### Результат:

10
20

### 8.2 `filter(Predicate predicate)`

Пропускает только элементы, удовлетворяющие условию.

### Пример:

```Observable.<Integer>create(emitter -> {
    emitter.onNext(10);
    emitter.onNext(15);
    emitter.onNext(20);
    emitter.onComplete();
}).filter(x -> x >= 15);
```

### Результат:

15
20

### 8.3 `flatMap(Function<T, Observable<R>> mapper)`

Для каждого элемента внешнего потока создаёт внутренний Observable, а затем объединяет все внутренние потоки в
единый downstream-поток.

### Особенности текущей реализации

- поддерживаются несколько внутренних подписок;
- завершение происходит только после завершения внешнего потока и всех внутренних подписок;
- первая ошибка завершает весь flatMap;
- downstream-сигналы сериализуются через SerializedObserver, чтобы избежать конкурентных вызовов observer.onNext(...).

### Почему это важно

Без сериализации два внутренних источника могли бы одновременно вызвать downstream observer. Это особенно опасно в
многопоточной среде. В текущей реализации этот риск устранён.

## 9. Управление потоками выполнения

### 9.1 `subscribeOn(...)`

Меняет поток, в котором выполняется подписка на upstream-source. То есть влияет на то, где запускается источник.

### Пример:

```Observable.<Integer>create(emitter -> {
    System.out.println("source thread = " + Thread.currentThread().getName());
    emitter.onNext(1);
    emitter.onComplete();
}).subscribeOn(Schedulers.io());
```

### 9.2 `observeOn(...)`

Меняет поток, в котором downstream observer получает сигналы. То есть влияет на то, где выполняются onNext, onError,
onComplete у подписчика.

### Важная гарантия реализации

observeOn реализован не наивно через схему «одна задача executor-а на один сигнал», а через очередь сигналов,
wip-счётчик и single-consumer drain loop.

### Это даёт следующие гарантии:

- сохраняется порядок onNext;
- onComplete и onError не обгоняют onNext;
- downstream не вызывается конкурентно;
- после terminal-сигнала новые onNext не доставляются.

## 10. Scheduler-ы и различия между ними

`IOThreadScheduler`
реализован через newCachedThreadPool();
подходит для сетевых запросов, файлового ввода-вывода, обращений к БД, I/O-bound задач, где потоки часто ждут внешний
ресурс;
использовать, когда задача большую часть времени не вычисляет, а ждёт.

`ComputationScheduler`
реализован через newFixedThreadPool(...) размером по числу доступных ядер;
подходит для вычислительных задач, CPU-bound обработки, преобразования данных, параллельных вычислений без блокирующего
I/O;
использовать, когда задача нагружает процессор.

`SingleThreadScheduler`
реализован через newSingleThreadExecutor();
подходит для последовательной обработки, гарантированного порядка, сценариев, где важно отсутствие конкурентного
доступа;
использовать, когда нужно строгое последовательное выполнение в одном потоке.

`Schedulers.shutdown() и Schedulers.reset()`

В проект добавлены методы lifecycle-управления singleton scheduler-ами:

- `shutdown()` — завершает текущие singleton scheduler-ы;
- `reset()` — пересоздаёт их заново.

### Это полезно для:

- тестовой изоляции;
- повторных прогонов тестов;
- предсказуемого состояния окружения.

## 11. Потокобезопасность и модель сигналов

### 11.1 `Terminal-сигналы`

### В библиотеке соблюдается базовое правило реактивного контракта:

- после onError() больше нельзя отправлять onNext() или onComplete();
- после onComplete() больше нельзя отправлять onNext() или onError().
- onError и onComplete являются terminal-сигналами.

### 11.2 `Single terminal signal`

Каждый поток должен завершиться только одним terminal-сигналом.

### Это обеспечивается:

- флагами terminated;
- защитой в CreateEmitter;
- сериализацией сигналов;
- координацией завершения в flatMap;
- очередью сигналов в observeOn.

### 11.3 Где именно гарантируется сериализация сигналов

В `flatMap`

Сигналы от нескольких inner-источников сходятся в один downstream-поток. Чтобы избежать конкурентных вызовов downstream
observer-а, используется SerializedObserver.

В `observeOn`

Сигналы складываются в очередь и выгружаются через один drain loop. Это гарантирует:

- отсутствие параллельных downstream-вызовов;
- порядок событий;
- корректную доставку terminal-сигнала.

### 11.4 Исключения из `downstream observer`

Если downstream onNext(...) выбрасывает исключение:

- поток переводится в ошибку;
- выполняется отмена подписок;
- downstream получает onError(...).

Если исключение возникает внутри terminal-callback, в учебной реализации оно не эскалируется глобально и
подавляется после попытки доставки terminal-сигнала. Это осознанное упрощение относительно production-библиотеки.

## 12. Обработка ошибок

### Ошибки могут возникнуть:

- в source (Observable.create(...));
- в map(...);
- в filter(...);
- во внутреннем Observable внутри flatMap(...);
- в downstream observer.

Во всех основных сценариях они переводятся в onError(...).

### Семантика

- первая ошибка завершает поток;
- после ошибки terminal-состояние считается достигнутым;
- новые элементы больше не доставляются.

## 13.

### Сценарий 1. Базовый pipeline

```
Observable.<Integer>create(emitter -> {
emitter.onNext(1);
emitter.onNext(2);
emitter.onNext(3);
emitter.onComplete();
})
.map(x -> x * 10)
.filter(x -> x >= 20)
.subscribe(new Observer<Integer>() {
@Override
public void onNext(Integer item) {
System.out.println(item);
}

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public void onComplete() {
        System.out.println("done");
    }
});
```

### Сценарий 2. Переключение потоков

```
Observable.<Integer>create(emitter -> {
    System.out.println("source thread = " + Thread.currentThread().getName());
    emitter.onNext(1);
    emitter.onComplete();
})
.subscribeOn(Schedulers.io())
.observeOn(Schedulers.single())
.subscribe(new Observer<Integer>() {
    @Override
    public void onNext(Integer item) {
        System.out.println("observer thread = " + Thread.currentThread().getName());
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public void onComplete() {
        System.out.println("completed");
    }
});
```

### Сценарий 3. flatMap с несколькими inner-источниками

```
Observable.<Integer>create(emitter -> {
    emitter.onNext(1);
    emitter.onNext(2);
    emitter.onComplete();
})
.flatMap(value -> Observable.<String>create(inner -> {
    inner.onNext("value=" + value);
    inner.onNext("mapped=" + (value * 10));
    inner.onComplete();
}))
.subscribe(new Observer<String>() {
    @Override
    public void onNext(String item) {
        System.out.println(item);
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public void onComplete() {
        System.out.println("done");
    }
});
```

### Сценарий 4. Ошибка в map

```
Observable.<Integer>create(emitter -> {
    emitter.onNext(1);
    emitter.onComplete();
})
.map(x -> {
    throw new IllegalArgumentException("map failure");
})
.subscribe(new Observer<Object>() {
    @Override
    public void onNext(Object item) {
    }

    @Override
    public void onError(Throwable t) {
        System.out.println("error = " + t.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("completed");
    }
});
```

## 14.Тестирование

В проекте есть unit-тесты для:

### Базовых сценариев

создание потока и подписка;
map;
filter;
flatMap;
обработка ошибок;
Disposable;
scheduler-ы.

### Многопоточных сценариев

конкурентные inner-emissions в flatMap;
сериализация downstream-вызовов;
порядок событий в observeOn;
отсутствие onNext после onError;
завершение flatMap по первой ошибке;
lifecycle singleton scheduler-ов.

### Актуальный локальный прогон

Проект локально успешно проходит:
`mvn clean test`
`mvn clean package`
Количество тестов в актуальной версии: 18.

Это подтверждает не только наличие тестов, но и их успешный прогон в рабочем окружении.

## 15. Команды сборки и запуска

### Компиляция

`mvn clean compile`

### Запуск тестов

`mvn clean test`

### Сборка jar

`mvn clean package`

### Запуск demo

`java -cp target/classes ru.rxclone.demo.DemoMain`

## 16. Demo

Основной демонстрационный класс:
`ru.rxclone.demo.DemoMain`
Пример ожидаемого вывода:
`rx-single-1 -> value=20
rx-single-1 -> value=30
Completed`

## 17. Вывод:

В рамках задания реализована упрощённая RxJava-подобная библиотека, которая включает:

- базовые reactive-компоненты;
- цепочку операторов;
- управление потоками выполнения;
- обработку ошибок;
- отмену подписки;
- сериализацию сигналов в конкурентных сценариях;
- unit-тесты, включая многопоточные проверки.

Проект соответствует требованиям задания, демонстрирует понимание реактивной модели и содержит архитектурные элементы,
характерные для реальных многопоточных библиотек.

