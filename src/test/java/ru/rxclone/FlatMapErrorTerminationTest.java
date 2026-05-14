package ru.rxclone;

import org.junit.jupiter.api.Test;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlatMapErrorTerminationTest {

    @Test
    void flatMapShouldTerminateOnceOnFirstError() throws InterruptedException {
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).flatMap(value -> {
            if (value == 2) {
                return Observable.<String>create(inner -> {
                    Thread thread = new Thread(() -> {
                        try {
                            Thread.sleep(20);
                            inner.onError(new IllegalStateException("inner boom"));
                        } catch (Throwable throwable) {
                            inner.onError(throwable);
                        }
                    });
                    thread.start();
                });
            }

            return Observable.<String>create(inner -> {
                Thread thread = new Thread(() -> {
                    try {
                        inner.onNext("value=" + value);
                        Thread.sleep(50);
                        inner.onNext("late=" + value);
                        inner.onComplete();
                    } catch (Throwable throwable) {
                        inner.onError(throwable);
                    }
                });
                thread.start();
            });
        }).subscribe(new Observer<String>() {
            @Override
            public void onNext(String item) {
                events.add(item);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                events.add("E:" + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onComplete() {
                completed.set(true);
                events.add("C");
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "flatMap did not terminate in time");

        Thread.sleep(150);

        assertEquals("inner boom", errorRef.get().getMessage());
        assertFalse(completed.get());
        assertTrue(events.contains("E:inner boom"));
        assertFalse(events.contains("C"));
        assertFalse(events.contains("late=1"));
        assertFalse(events.contains("late=3"));
    }
}