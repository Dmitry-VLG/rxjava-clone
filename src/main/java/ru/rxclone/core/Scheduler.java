package ru.rxclone.core;

public interface Scheduler {
    void execute(Runnable task);
}