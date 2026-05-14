package ru.rxclone;

import org.junit.jupiter.api.Test;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.core.Scheduler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObserveOnErrorTerminationTest {

    @Test
    void observeOnShouldNotCallOnNextAfterOnError() throws InterruptedException {
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Scheduler asyncScheduler = task -> {
            Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            });
            thread.start();
        };

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onError(new IllegalStateException("boom"));
                    emitter.onNext(3);
                    emitter.onComplete();
                }).observeOn(asyncScheduler)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        events.add("N" + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        errorRef.set(t);
                        events.add("E:" + t.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        events.add("C");
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "observeOn did not terminate in time");
        assertEquals(List.of("N1", "N2", "E:boom"), events);
    }
}