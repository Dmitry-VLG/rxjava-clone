package ru.rxclone.observables;

import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

public class ObservableFilter<T> extends Observable<T> {

    private final Observable<T> source;
    private final Predicate<? super T> predicate;

    public ObservableFilter(Observable<T> source, Predicate<? super T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected Disposable subscribeActual(Observer<? super T> observer) {
        AtomicBoolean done = new AtomicBoolean(false);

        return source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (done.get()) {
                    return;
                }

                try {
                    if (predicate.test(item)) {
                        observer.onNext(item);
                    }
                } catch (Throwable throwable) {
                    if (done.compareAndSet(false, true)) {
                        observer.onError(throwable);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                if (done.compareAndSet(false, true)) {
                    observer.onError(t);
                }
            }

            @Override
            public void onComplete() {
                if (done.compareAndSet(false, true)) {
                    observer.onComplete();
                }
            }
        });
    }
}