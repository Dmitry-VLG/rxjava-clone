package ru.rxclone;

import org.junit.jupiter.api.Test;
import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlatMapOperatorTest {

    @Test
    void flatMapShouldFlattenInnerObservables() throws InterruptedException {
        List<Integer> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).flatMap(value -> Observable.<Integer>create(inner -> {
            inner.onNext(value);
            inner.onNext(value * 10);
            inner.onComplete();
        })).subscribe(new Observer<Integer>() {
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
                latch.countDown();
            }
        });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of(1, 10, 2, 20), received);
    }
}