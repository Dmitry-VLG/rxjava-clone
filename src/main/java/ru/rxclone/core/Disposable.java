package ru.rxclone.core;

public interface Disposable {
    void dispose();

    boolean isDisposed();
}