package ru.rxclone.internal;

import ru.rxclone.core.Emitter;
import ru.rxclone.core.Observer;

import java.util.concurrent.atomic.AtomicBoolean;

public class CreateEmitter<T> implements Emitter<T> {

    private final Observer<? super T> downstream;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public CreateEmitter(Observer<? super T> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onNext(T item) {
        if (disposed.get() || terminated.get()) {
            return;
        }

        if (item == null) {
            onError(new NullPointerException("onNext item must not be null"));
            return;
        }

        try {
            downstream.onNext(item);
        } catch (Throwable throwable) {
            onError(throwable);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (disposed.get()) {
            return;
        }

        Throwable error = t != null ? t : new NullPointerException("Throwable must not be null");

        if (terminated.compareAndSet(false, true)) {
            try {
                downstream.onError(error);
            } finally {
                dispose();
            }
        }
    }

    @Override
    public void onComplete() {
        if (disposed.get()) {
            return;
        }

        if (terminated.compareAndSet(false, true)) {
            try {
                downstream.onComplete();
            } finally {
                dispose();
            }
        }
    }

    @Override
    public void dispose() {
        disposed.set(true);
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}