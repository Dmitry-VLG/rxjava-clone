package ru.rxclone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.rxclone.core.Scheduler;
import ru.rxclone.schedulers.Schedulers;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class SchedulersLifecycleTest {

    @AfterEach
    void tearDown() {
        Schedulers.reset();
    }

    @Test
    void schedulersShouldReturnSingletonInstancesUntilReset() {
        Scheduler io1 = Schedulers.io();
        Scheduler io2 = Schedulers.io();

        Scheduler computation1 = Schedulers.computation();
        Scheduler computation2 = Schedulers.computation();

        Scheduler single1 = Schedulers.single();
        Scheduler single2 = Schedulers.single();

        assertSame(io1, io2);
        assertSame(computation1, computation2);
        assertSame(single1, single2);
    }

    @Test
    void resetShouldRecreateSchedulerInstances() {
        Scheduler oldIo = Schedulers.io();
        Scheduler oldComputation = Schedulers.computation();
        Scheduler oldSingle = Schedulers.single();

        Schedulers.reset();

        Scheduler newIo = Schedulers.io();
        Scheduler newComputation = Schedulers.computation();
        Scheduler newSingle = Schedulers.single();

        assertNotSame(oldIo, newIo);
        assertNotSame(oldComputation, newComputation);
        assertNotSame(oldSingle, newSingle);
    }
}