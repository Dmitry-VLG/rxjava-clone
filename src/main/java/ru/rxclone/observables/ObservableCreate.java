package ru.rxclone.observables;

import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.ObservableOnSubscribe;
import ru.rxclone.core.Observer;
import ru.rxclone.internal.CreateEmitter;

public class ObservableCreate<T> extends Observable<T> {

    private final ObservableOnSubscribe<T> source;

    public ObservableCreate(ObservableOnSubscribe<T> source) {
        this.source = source;
    }

    @Override
    protected Disposable subscribeActual(Observer<? super T> observer) {
        CreateEmitter<T> emitter = new CreateEmitter<>(observer);
        try {
            source.subscribe(emitter);
        } catch (Throwable throwable) {
            emitter.onError(throwable);
        }
        return emitter;
    }
}