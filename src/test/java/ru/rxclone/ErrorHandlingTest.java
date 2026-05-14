package ru.rxclone;

import org.junit.jupiter.api.Test;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorHandlingTest {

    @Test
    void sourceErrorShouldBeDeliveredToOnError() {
        AtomicReference<String> errorMessage = new AtomicReference<>();

        Observable.create(emitter -> {
            throw new IllegalStateException("source failure");
        }).subscribe(new Observer<Object>() {
            @Override
            public void onNext(Object item) {
            }

            @Override
            public void onError(Throwable t) {
                errorMessage.set(t.getMessage());
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals("source failure", errorMessage.get());
    }

    @Test
    void mapErrorShouldBeDeliveredToOnError() {
        AtomicReference<String> errorMessage = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        }).map(x -> {
            throw new IllegalArgumentException("map failure");
        }).subscribe(new Observer<Object>() {
            @Override
            public void onNext(Object item) {
            }

            @Override
            public void onError(Throwable t) {
                errorMessage.set(t.getMessage());
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        assertEquals("map failure", errorMessage.get());
        assertEquals(false, completed.get());
    }

    @Test
    void filterErrorShouldBeDeliveredToOnError() {
        AtomicReference<String> errorMessage = new AtomicReference<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(5);
            emitter.onComplete();
        }).filter(x -> {
            throw new IllegalArgumentException("filter failure");
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
            }

            @Override
            public void onError(Throwable t) {
                errorMessage.set(t.getMessage());
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals("filter failure", errorMessage.get());
    }

    @Test
    void flatMapInnerErrorShouldBeDeliveredToOnError() {
        AtomicReference<String> errorMessage = new AtomicReference<>();

        Observable.create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        }).flatMap(value -> Observable.create(inner -> {
            throw new RuntimeException("inner failure");
        })).subscribe(new Observer<Object>() {
            @Override
            public void onNext(Object item) {
            }

            @Override
            public void onError(Throwable t) {
                errorMessage.set(t.getMessage());
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals("inner failure", errorMessage.get());
    }
}