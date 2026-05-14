package ru.rxclone.internal;

import ru.rxclone.core.Observer;

import java.util.ArrayDeque;

public final class SerializedObserver<T> implements Observer<T> {

    private static final Object COMPLETE = new Object();

    private static final class ErrorNotification {
        private final Throwable error;

        private ErrorNotification(Throwable error) {
            this.error = error;
        }
    }

    private final Observer<? super T> downstream;
    private final Runnable cancelAction;
    private final ArrayDeque<Object> queue = new ArrayDeque<>();

    private boolean emitting;
    private boolean terminated;

    public SerializedObserver(Observer<? super T> downstream, Runnable cancelAction) {
        this.downstream = downstream;
        this.cancelAction = cancelAction;
    }

    @Override
    public void onNext(T item) {
        if (item == null) {
            onError(new NullPointerException("onNext item must not be null"));
            return;
        }

        Object signal;

        synchronized (this) {
            if (terminated) {
                return;
            }

            if (emitting) {
                queue.offer(item);
                return;
            }

            emitting = true;
            signal = item;
        }

        drain(signal);
    }

    @Override
    public void onError(Throwable t) {
        Throwable error = t != null ? t : new NullPointerException("Throwable must not be null");
        Object signal = new ErrorNotification(error);

        synchronized (this) {
            if (terminated) {
                return;
            }

            terminated = true;

            if (emitting) {
                queue.offer(signal);
                return;
            }

            emitting = true;
        }

        drain(signal);
    }

    @Override
    public void onComplete() {
        synchronized (this) {
            if (terminated) {
                return;
            }

            terminated = true;

            if (emitting) {
                queue.offer(COMPLETE);
                return;
            }

            emitting = true;
        }

        drain(COMPLETE);
    }

    @SuppressWarnings("unchecked")
    private void drain(Object firstSignal) {
        Object signal = firstSignal;

        for (;;) {
            if (signal == COMPLETE) {
                safeComplete();
                return;
            }

            if (signal instanceof ErrorNotification errorNotification) {
                safeError(errorNotification.error);
                return;
            }

            try {
                downstream.onNext((T) signal);
            } catch (Throwable throwable) {
                safeError(throwable);
                return;
            }

            synchronized (this) {
                signal = queue.poll();
                if (signal == null) {
                    emitting = false;
                    return;
                }
            }
        }
    }

    private void safeComplete() {
        try {
            downstream.onComplete();
        } catch (Throwable throwable) {
            safeError(throwable);
        }
    }

    private void safeError(Throwable throwable) {
        runCancelAction();

        try {
            downstream.onError(throwable);
        } catch (Throwable ignored) {
            // Intentionally ignored: downstream already failed during terminal delivery.
        }
    }

    private void runCancelAction() {
        if (cancelAction == null) {
            return;
        }

        try {
            cancelAction.run();
        } catch (Throwable ignored) {
            // Ignore cancel action failures.
        }
    }
}