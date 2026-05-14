package ru.rxclone.observables;

import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ObservableMap<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<? super T, ? extends R> mapper;

    public ObservableMap(Observable<T> source, Function<? super T, ? extends R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected Disposable subscribeActual(Observer<? super R> observer) {
        AtomicBoolean done = new AtomicBoolean(false);

        return source.subscribe(new Observer<T>() {
            @Override
            public void onNext(T item) {
                if (done.get()) {
                    return;
                }

                try {
                    R mapped = Objects.requireNonNull(mapper.apply(item), "Mapper returned null");
                    observer.onNext(mapped);
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