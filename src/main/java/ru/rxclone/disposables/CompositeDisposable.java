package ru.rxclone.disposables;

import ru.rxclone.core.Disposable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class CompositeDisposable implements Disposable {

    private final List<Disposable> disposables = new CopyOnWriteArrayList<>();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    public void add(Disposable disposable) {
        if (disposable == null) {
            return;
        }

        if (disposed.get()) {
            disposable.dispose();
            return;
        }

        disposables.add(disposable);

        if (disposed.get()) {
            disposables.remove(disposable);
            disposable.dispose();
        }
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            for (Disposable disposable : disposables) {
                disposable.dispose();
            }
            disposables.clear();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}