package ru.rxclone.observables;

import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.core.Scheduler;
import ru.rxclone.disposables.BooleanDisposable;
import ru.rxclone.disposables.CompositeDisposable;

public class ObservableSubscribeOn<T> extends Observable<T> {

    private final Observable<T> source;
    private final Scheduler scheduler;

    public ObservableSubscribeOn(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected Disposable subscribeActual(Observer<? super T> observer) {
        CompositeDisposable composite = new CompositeDisposable();
        BooleanDisposable scheduledDisposable = new BooleanDisposable();
        composite.add(scheduledDisposable);

        scheduler.execute(() -> {
            if (scheduledDisposable.isDisposed()) {
                return;
            }

            Disposable upstream = source.subscribe(observer);
            composite.add(upstream);
        });

        return composite;
    }
}