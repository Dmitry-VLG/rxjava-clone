# RxJava Clone

Учебная реализация упрощенной RxJava-подобной библиотеки на Java.

## Что реализовано

- Observer
- Observable
- create()
- map()
- filter()
- flatMap()
- Disposable
- Scheduler
- IOThreadScheduler
- ComputationScheduler
- SingleThreadScheduler
- subscribeOn()
- observeOn()
- обработка ошибок
- unit-тесты

## Архитектура

### Observable
Базовый абстрактный класс, представляющий источник событий.

### Observer
Подписчик, который получает события:
- onNext
- onError
- onComplete

### Disposable
Интерфейс для отмены подписки.

### Scheduler
Абстракция выполнения задач в потоке или пуле потоков.

### Операторы
Операторы реализованы как отдельные классы-обертки над исходным Observable:
- ObservableMap
- ObservableFilter
- ObservableFlatMap
- ObservableSubscribeOn
- ObservableObserveOn

## Реализации Scheduler

### IOThreadScheduler
Использует CachedThreadPool.
Подходит для I/O-операций:
- сетевые запросы
- чтение/запись файлов
- обращения к БД

### ComputationScheduler
Использует FixedThreadPool размером по числу доступных ядер.
Подходит для вычислительных задач.

### SingleThreadScheduler
Использует один поток.
Подходит для последовательной обработки событий.

## Тестирование

Покрыты тестами:
- базовая подписка
- map
- filter
- flatMap
- обработка ошибок
- Disposable
- subscribeOn / observeOn
- работа Scheduler в многопоточной среде



