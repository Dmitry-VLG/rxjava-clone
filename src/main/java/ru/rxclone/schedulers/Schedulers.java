package ru.rxclone.schedulers;

import ru.rxclone.core.Scheduler;

public final class Schedulers {

    private static final IOThreadScheduler IO = new IOThreadScheduler();
    private static final ComputationScheduler COMPUTATION = new ComputationScheduler();
    private static final SingleThreadScheduler SINGLE = new SingleThreadScheduler();

    private Schedulers() {
    }

    public static Scheduler io() {
        return IO;
    }

    public static Scheduler computation() {
        return COMPUTATION;
    }

    public static Scheduler single() {
        return SINGLE;
    }
}