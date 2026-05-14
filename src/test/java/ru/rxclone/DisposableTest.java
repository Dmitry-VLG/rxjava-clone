package ru.rxclone;

import org.junit.jupiter.api.Test;
import ru.rxclone.core.Disposable;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.core.Scheduler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DisposableTest {

    @Test
    void disposeShouldCancelScheduledSubscriptionBeforeExecution() throws InterruptedException {
        List<Integer> received = new CopyOnWriteArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Scheduler delayedScheduler = task -> {
            Thread thread = new Thread(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            });
            thread.start();
        };

        Disposable disposable = Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .subscribeOn(delayedScheduler)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        received.add(item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new AssertionError("Error should not happen", t);
                    }

                    @Override
                    public void onComplete() {
                        completed.set(true);
                    }
                });

        disposable.dispose();

        Thread.sleep(200);

        assertTrue(disposable.isDisposed());
        assertTrue(received.isEmpty());
        assertTrue(!completed.get());
    }
}