package ru.rxclone.demo;

import ru.rxclone.core.Observable;
import ru.rxclone.core.Observer;
import ru.rxclone.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;

public class DemoMain {

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                })
                .map(x -> x * 10)
                .filter(x -> x >= 20)
                .flatMap(x -> Observable.<String>create(inner -> {
                    inner.onNext("value=" + x);
                    inner.onComplete();
                }))
                .subscribeOn(Schedulers.io())
                .observeOn(Schedulers.single())
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String item) {
                        System.out.println(Thread.currentThread().getName() + " -> " + item);
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Completed");
                        latch.countDown();
                    }
                });

        latch.await();
    }
}