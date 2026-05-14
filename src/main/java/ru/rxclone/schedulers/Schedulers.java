package ru.rxclone.schedulers;

import ru.rxclone.core.Scheduler;

public final class Schedulers {

    private static volatile IOThreadScheduler io = new IOThreadScheduler();
    private static volatile ComputationScheduler computation = new ComputationScheduler();
    private static volatile SingleThreadScheduler single = new SingleThreadScheduler();

    private Schedulers() {
    }

    public static Scheduler io() {
        return io;
    }

    public static Scheduler computation() {
        return computation;
    }

    public static Scheduler single() {
        return single;
    }

    public static synchronized void shutdown() {
        io.shutdown();
        computation.shutdown();
        single.shutdown();
    }

    public static synchronized void reset() {
        shutdown();
        io = new IOThreadScheduler();
        computation = new ComputationScheduler();
        single = new SingleThreadScheduler();
    }
}