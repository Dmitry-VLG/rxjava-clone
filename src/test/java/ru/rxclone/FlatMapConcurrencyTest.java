package ru.rxclone;

import org.junit.jupiter.api.Test;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlatMapConcurrencyTest {

    @Test
    void flatMapShouldSerializeConcurrentInnerEmissions() throws InterruptedException {
        int outerCount = 20;
        int itemsPerInner = 2;

        List<Integer> received = new CopyOnWriteArrayList<>();
        AtomicInteger activeOnNextCalls = new AtomicInteger(0);
        AtomicBoolean concurrentOnNextDetected = new AtomicBoolean(false);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        CountDownLatch completeLatch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            for (int i = 1; i <= outerCount; i++) {
                emitter.onNext(i);
            }
            emitter.onComplete();
        }).flatMap(value -> Observable.<Integer>create(inner -> {
            Thread thread = new Thread(() -> {
                try {
                    inner.onNext(value);
                    Thread.sleep(5);
                    inner.onNext(value * 100);
                    inner.onComplete();
                } catch (Throwable throwable) {
                    inner.onError(throwable);
                }
            });
            thread.start();
        })).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                int inFlight = activeOnNextCalls.incrementAndGet();
                if (inFlight > 1) {
                    concurrentOnNextDetected.set(true);
                }

                try {
                    received.add(item);
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    errorRef.set(e);
                } finally {
                    activeOnNextCalls.decrementAndGet();
                }
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                completeLatch.countDown();
            }

            @Override
            public void onComplete() {
                completeLatch.countDown();
            }
        });

        assertTrue(completeLatch.await(5, TimeUnit.SECONDS), "flatMap did not complete in time");
        assertNull(errorRef.get(), "Unexpected error in flatMap");
        assertFalse(concurrentOnNextDetected.get(), "Downstream onNext was called concurrently");
        assertEquals(outerCount * itemsPerInner, received.size(), "Unexpected number of received items");
    }
}