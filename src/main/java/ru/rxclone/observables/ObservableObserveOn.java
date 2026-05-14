package ru.rxclone.observables;

import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.core.Scheduler;
import ru.rxclone.disposables.CompositeDisposable;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ObservableObserveOn<T> extends Observable<T> {

    public ObservableObserveOn(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    private final Observable<T> source;
    private final Scheduler scheduler;

    @Override
    protected Disposable subscribeActual(Observer<? super T> observer) {
        ObserveOnObserver<T> parent = new ObserveOnObserver<>(observer, scheduler);
        Disposable upstream = source.subscribe(parent);
        parent.setUpstream(upstream);
        return parent;
    }

    private static final class ObserveOnObserver<T> implements Observer<T>, Disposable, Runnable {

        private static final Object COMPLETE = new Object();

        private static final class ErrorNotification {
            private final Throwable error;

            private ErrorNotification(Throwable error) {
                this.error = error;
            }
        }

        private final Observer<? super T> downstream;
        private final Scheduler scheduler;
        private final CompositeDisposable composite = new CompositeDisposable();
        private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();
        private final AtomicInteger wip = new AtomicInteger(0);
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        private ObserveOnObserver(Observer<? super T> downstream, Scheduler scheduler) {
            this.downstream = downstream;
            this.scheduler = scheduler;
        }

        private void setUpstream(Disposable upstream) {
            composite.add(upstream);
        }

        @Override
        public void onNext(T item) {
            if (item == null) {
                onError(new NullPointerException("onNext item must not be null"));
                return;
            }

            if (terminated.get() || composite.isDisposed()) {
                return;
            }

            queue.offer(item);
            schedule();
        }

        @Override
        public void onError(Throwable t) {
            Throwable error = t != null ? t : new NullPointerException("Throwable must not be null");

            if (terminated.compareAndSet(false, true)) {
                queue.offer(new ErrorNotification(error));
                schedule();
            }
        }

        @Override
        public void onComplete() {
            if (terminated.compareAndSet(false, true)) {
                queue.offer(COMPLETE);
                schedule();
            }
        }

        private void schedule() {
            if (wip.getAndIncrement() == 0) {
                scheduler.execute(this);
            }
        }

        @Override
        public void run() {
            int missed = 1;

            for (;;) {
                if (composite.isDisposed()) {
                    queue.clear();
                    return;
                }

                for (;;) {
                    Object signal = queue.poll();
                    if (signal == null) {
                        break;
                    }

                    if (signal == COMPLETE) {
                        composite.dispose();
                        safeComplete();
                        return;
                    }

                    if (signal instanceof ErrorNotification errorNotification) {
                        composite.dispose();
                        safeError(errorNotification.error);
                        return;
                    }

                    if (!safeNext(signal)) {
                        return;
                    }
                }

                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        @SuppressWarnings("unchecked")
        private boolean safeNext(Object signal) {
            try {
                downstream.onNext((T) signal);
                return true;
            } catch (Throwable throwable) {
                composite.dispose();
                queue.clear();
                safeError(throwable);
                return false;
            }
        }

        private void safeError(Throwable throwable) {
            try {
                downstream.onError(throwable);
            } catch (Throwable ignored) {
                // Terminal error delivery failed in downstream.
            }
        }

        private void safeComplete() {
            try {
                downstream.onComplete();
            } catch (Throwable throwable) {
                safeError(throwable);
            }
        }

        @Override
        public void dispose() {
            composite.dispose();
            queue.clear();
        }

        @Override
        public boolean isDisposed() {
            return composite.isDisposed();
        }
    }
}