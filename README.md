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

| Требование | Реализация | Статус |
|---|---|---|
| `Observer` с `onNext/onError/onComplete` | `ru.rxclone.core.Observer` | Выполнено |
| `Observable` с `subscribe(...)` | `ru.rxclone.core.Observable` | Выполнено |
| `Observable.create(...)` | `Observable.create(...)` + `ObservableCreate` | Выполнено |
| `map(...)` | `ObservableMap` | Выполнено |
| `filter(...)` | `ObservableFilter` | Выполнено |
| `Scheduler.execute(...)` | `ru.rxclone.core.Scheduler` | Выполнено |
| `IOThreadScheduler` | `ru.rxclone.schedulers.IOThreadScheduler` | Выполнено |
| `ComputationScheduler` | `ru.rxclone.schedulers.ComputationScheduler` | Выполнено |
| `SingleThreadScheduler` | `ru.rxclone.schedulers.SingleThreadScheduler` | Выполнено |
| `subscribeOn(...)` | `ObservableSubscribeOn` | Выполнено |
| `observeOn(...)` | `ObservableObserveOn` | Выполнено |
| `flatMap(...)` | `ObservableFlatMap` | Выполнено |
| `Disposable` | `Disposable`, `BooleanDisposable`, `CompositeDisposable`, `CreateEmitter` | Выполнено |
| Обработка ошибок через `onError` | `CreateEmitter`, операторы, сериализация terminal-сигналов | Выполнено |
| Unit-тесты | `src/test/java/ru/rxclone` | Выполнено |

---

## 4. Архитектура 

### Структура 

```text
ru.rxclone
├── core
│   ├── Disposable
│   ├── Emitter
│   ├── Observable
│   ├── ObservableOnSubscribe
│   ├── Observer
│   └── Scheduler
├── disposables
│   ├── BooleanDisposable
│   └── CompositeDisposable
├── internal
│   ├── CreateEmitter
│   └── SerializedObserver
├── observables
│   ├── ObservableCreate
│   ├── ObservableMap
│   ├── ObservableFilter
│   ├── ObservableFlatMap
│   ├── ObservableSubscribeOn
│   └── ObservableObserveOn
├── schedulers
│   ├── IOThreadScheduler
│   ├── ComputationScheduler
│   ├── SingleThreadScheduler
│   └── Schedulers
└── demo
    └── DemoMain