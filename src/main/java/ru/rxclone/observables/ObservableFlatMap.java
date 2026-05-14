package ru.rxclone.observables;

import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.disposables.CompositeDisposable;
import ru.rxclone.internal.SerializedObserver;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ObservableFlatMap<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<? super T, ? extends Observable<? extends R>> mapper;

    public ObservableFlatMap(
            Observable<T> source,
            Function<? super T, ? extends Observable<? extends R>> mapper
    ) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected Disposable subscribeActual(Observer<? super R> observer) {
        FlatMapObserver<T, R> parent = new FlatMapObserver<>(observer, mapper);
        Disposable upstream = source.subscribe(parent);
        parent.setUpstream(upstream);
        return parent;
    }

    private static final class FlatMapObserver<T, R> implements Observer<T>, Disposable {

        private final Function<? super T, ? extends Observable<? extends R>> mapper;
        private final CompositeDisposable disposables = new CompositeDisposable();
        private final AtomicInteger activeCount = new AtomicInteger(1);
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final SerializedObserver<R> downstream;

        private FlatMapObserver(
                Observer<? super R> downstreamObserver,
                Function<? super T, ? extends Observable<? extends R>> mapper
        ) {
            this.mapper = mapper;
            this.downstream = new SerializedObserver<>(
                    downstreamObserver,
                    () -> {
                        terminated.set(true);
                        disposables.dispose();
                    }
            );
        }

        private void setUpstream(Disposable upstream) {
            disposables.add(upstream);
        }

        @Override
        public void onNext(T item) {
            if (terminated.get() || disposables.isDisposed()) {
                return;
            }

            final Observable<? extends R> innerSource;
            try {
                innerSource = Objects.requireNonNull(
                        mapper.apply(item),
                        "flatMap mapper returned null"
                );
            } catch (Throwable throwable) {
                onError(throwable);
                return;
            }

            activeCount.incrementAndGet();

            Disposable innerDisposable = innerSource.subscribe(new Observer<R>() {
                @Override
                public void onNext(R innerItem) {
                    if (terminated.get() || disposables.isDisposed()) {
                        return;
                    }

                    downstream.onNext(innerItem);
                }

                @Override
                public void onError(Throwable t) {
                    if (terminated.compareAndSet(false, true)) {
                        disposables.dispose();
                        downstream.onError(t);
                    }
                }

                @Override
                public void onComplete() {
                    tryTerminate();
                }
            });

            disposables.add(innerDisposable);
        }

        @Override
        public void onError(Throwable t) {
            if (terminated.compareAndSet(false, true)) {
                disposables.dispose();
                downstream.onError(t);
            }
        }

        @Override
        public void onComplete() {
            tryTerminate();
        }

        private void tryTerminate() {
            if (activeCount.decrementAndGet() == 0
                    && terminated.compareAndSet(false, true)) {
                downstream.onComplete();
            }
        }

        @Override
        public void dispose() {
            terminated.set(true);
            disposables.dispose();
        }

        @Override
        public boolean isDisposed() {
            return disposables.isDisposed();
        }
    }
}