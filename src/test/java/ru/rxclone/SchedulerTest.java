package ru.rxclone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.schedulers.ComputationScheduler;
import ru.rxclone.schedulers.IOThreadScheduler;
import ru.rxclone.schedulers.SingleThreadScheduler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchedulerTest {

    private final IOThreadScheduler ioScheduler = new IOThreadScheduler();
    private final ComputationScheduler computationScheduler = new ComputationScheduler();
    private final SingleThreadScheduler singleThreadScheduler = new SingleThreadScheduler();

    @AfterEach
    void tearDown() {
        ioScheduler.shutdown();
        computationScheduler.shutdown();
        singleThreadScheduler.shutdown();
    }

    @Test
    void subscribeOnShouldMoveSourceWorkToAnotherThread() throws InterruptedException {
        String testThreadName = Thread.currentThread().getName();
        AtomicReference<String> sourceThreadName = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    sourceThreadName.set(Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                }).subscribeOn(ioScheduler)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNotEquals(testThreadName, sourceThreadName.get());
    }

    @Test
    void observeOnShouldMoveObserverWorkToAnotherThread() throws InterruptedException {
        String testThreadName = Thread.currentThread().getName();
        AtomicReference<String> observerThreadName = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onComplete();
                }).observeOn(ioScheduler)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        observerThreadName.set(Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertNotEquals(testThreadName, observerThreadName.get());
    }

    @Test
    void singleThreadSchedulerShouldReuseSingleThread() throws InterruptedException {
        Set<String> threadNames = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(3);

        singleThreadScheduler.execute(() -> {
            threadNames.add(Thread.currentThread().getName());
            latch.countDown();
        });

        singleThreadScheduler.execute(() -> {
            threadNames.add(Thread.currentThread().getName());
            latch.countDown();
        });

        singleThreadScheduler.execute(() -> {
            threadNames.add(Thread.currentThread().getName());
            latch.countDown();
        });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(1, threadNames.size());
    }
}