package ru.rxclone.core;

import ru.rxclone.observables.ObservableCreate;
import ru.rxclone.observables.ObservableFilter;
import ru.rxclone.observables.ObservableFlatMap;
import ru.rxclone.observables.ObservableMap;
import ru.rxclone.observables.ObservableObserveOn;
import ru.rxclone.observables.ObservableSubscribeOn;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class Observable<T> {

    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        Objects.requireNonNull(source, "source must not be null");
        return new ObservableCreate<>(source);
    }

    public final Disposable subscribe(Observer<? super T> observer) {
        Objects.requireNonNull(observer, "observer must not be null");
        return subscribeActual(observer);
    }

    protected abstract Disposable subscribeActual(Observer<? super T> observer);

    public final <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper, "mapper must not be null");
        return new ObservableMap<>(this, mapper);
    }

    public final Observable<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate, "predicate must not be null");
        return new ObservableFilter<>(this, predicate);
    }

    public final <R> Observable<R> flatMap(Function<? super T, ? extends Observable<? extends R>> mapper) {
        Objects.requireNonNull(mapper, "mapper must not be null");
        return new ObservableFlatMap<>(this, mapper);
    }

    public final Observable<T> subscribeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        return new ObservableSubscribeOn<>(this, scheduler);
    }

    public final Observable<T> observeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        return new ObservableObserveOn<>(this, scheduler);
    }
}