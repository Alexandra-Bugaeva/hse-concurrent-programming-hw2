package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Condition reaperChanged = lock.newCondition();
    private final ArrayDeque<Entry> idle = new ArrayDeque<>();
    private final Map<Entry, Boolean> borrowed = new IdentityHashMap<>();
    private final ArrayList<Waiter> waiters = new ArrayList<>();
    private final ConnectionFactory factory;
    private final int minIdle;
    private final Duration idleTimeout;
    private int maxSize;
    private final boolean fair;
    private int creating;
    private boolean stopped;
    private final Thread reaper;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.minIdle = config.minIdle();
        this.maxSize = config.maxSize();
        this.idleTimeout = config.idleTimeout();
        this.fair = config.fair();
        reaper = new Thread(this::reapLoop, "connection-pool-idle-reaper");
        reaper.setDaemon(true);
        reaper.start();
    }

    private PoolConfig config;

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) throw new IllegalArgumentException("timeout должен быть неотрицательным");
        long nanos = durationNanos(timeout);
        long start = System.nanoTime();
        Waiter waiter = null;
        lock.lockInterruptibly();
        try {
            if (stopped) throw new IllegalStateException("Пул остановлен");
            while (true) {
                if (stopped) {
                    removeWaiter(waiter);
                    throw new IllegalStateException("Пул остановлен");
                }
                boolean isHead = waiters.isEmpty() || waiters.get(0) == waiter;
                boolean mayTake = !fair || isHead;
                if (mayTake && !idle.isEmpty()) {
                    Entry entry = idle.removeFirst();
                    borrowed.put(entry, Boolean.TRUE);
                    removeWaiter(waiter);
                    signalEligible();
                    reaperChanged.signal();
                    return new Handle(entry);
                }
                int total = idle.size() + borrowed.size() + creating;
                if (mayTake && total < maxSize && (waiter == null || !fair || isHead)) {
                    creating++;
                    removeWaiter(waiter);
                    lock.unlock();
                    RawConnection raw;
                    try {
                        raw = Objects.requireNonNull(factory.create(), "factory returned null");
                    } catch (Throwable failure) {
                        lock.lock();
                        creating--;
                        signalEligible();
                        changed.signalAll();
                        if (failure instanceof Error error) throw error;
                        throw new ConnectionCreationException(failure);
                    }
                    lock.lock();
                    creating--;
                    if (stopped) {
                        changed.signalAll();
                        lock.unlock();
                        closeQuietly(raw);
                        lock.lock();
                        throw new IllegalStateException("Пул остановлен");
                    }
                    Entry entry = new Entry(raw);
                    borrowed.put(entry, Boolean.TRUE);
                    signalEligible();
                    changed.signalAll();
                    return new Handle(entry);
                }
                if (waiter == null) {
                    long remaining = nanos - (System.nanoTime() - start);
                    if (remaining <= 0) throw new PoolTimeoutException(timeout);
                    waiter = new Waiter(lock.newCondition());
                    waiters.add(waiter);
                }
                long remaining = nanos - (System.nanoTime() - start);
                if (remaining <= 0) {
                    removeWaiter(waiter);
                    signalEligible();
                    throw new PoolTimeoutException(timeout);
                }
                try {
                    waiter.condition.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    removeWaiter(waiter);
                    throw e;
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) lock.unlock();
        }
    }

    @Override
    public void resize(int newMaxSize) {
        if (newMaxSize < 1 || newMaxSize > PoolConfig.MAX_POOL_SIZE || newMaxSize < minIdle)
            throw new IllegalArgumentException("Недопустимый maxSize: " + newMaxSize);
        List<RawConnection> close = new ArrayList<>();
        lock.lock();
        try {
            if (stopped) throw new IllegalStateException("Пул остановлен");
            maxSize = newMaxSize;
            config = config.withMaxSize(newMaxSize);
            while (idle.size() + borrowed.size() + creating > maxSize && !idle.isEmpty())
                close.add(idle.removeLast().raw);
            signalEligible();
            changed.signalAll();
            reaperChanged.signal();
        } finally { lock.unlock(); }
        close.forEach(ConnectionPoolImpl::closeQuietly);
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try { return config; } finally { lock.unlock(); }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try { return new PoolStats(idle.size() + borrowed.size() + creating,
                idle.size(), borrowed.size(), creating, waiters.size()); }
        finally { lock.unlock(); }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        Objects.requireNonNull(grace, "grace");
        if (grace.isNegative()) throw new IllegalArgumentException("grace должен быть неотрицательным");
        long nanos = durationNanos(grace);
        long start = System.nanoTime();
        List<RawConnection> close = new ArrayList<>();
        lock.lockInterruptibly();
        try {
            // Только первый вызов выполняет остановку и ожидает grace. Повторный вызов — no-op.
            if (stopped) return;
            stopped = true;
            while (!idle.isEmpty()) close.add(idle.removeFirst().raw);
            for (Waiter waiter : waiters) waiter.condition.signal();
            changed.signalAll();
            reaperChanged.signal();
        } finally { lock.unlock(); }
        close.forEach(ConnectionPoolImpl::closeQuietly);
        boolean interrupted = false;
        lock.lock();
        try {
            while (!borrowed.isEmpty() && !interrupted) {
                long remaining = nanos - (System.nanoTime() - start);
                if (remaining <= 0) break;
                try { changed.awaitNanos(remaining); }
                catch (InterruptedException e) { interrupted = true; }
            }
            if (!borrowed.isEmpty()) {
                for (Entry entry : borrowed.keySet()) {
                    entry.closed = true;
                    close.add(entry.raw);
                }
                borrowed.clear();
            }
            changed.signalAll();
        } finally { lock.unlock(); }
        close.forEach(ConnectionPoolImpl::closeQuietly);
        try { reaper.join(); }
        catch (InterruptedException e) { interrupted = true; }
        if (interrupted) throw new InterruptedException();
    }

    private void returnEntry(Entry entry, Handle handle) {
        List<RawConnection> close = new ArrayList<>();
        lock.lock();
        try {
            if (handle.returned) return;
            handle.returned = true;
            if (!borrowed.containsKey(entry)) return;
            borrowed.remove(entry);
            if (stopped || idle.size() + borrowed.size() + creating >= maxSize) {
                entry.closed = true;
                close.add(entry.raw);
            } else {
                entry.idleSince = System.nanoTime();
                idle.addLast(entry);
                reaperChanged.signal();
            }
            signalEligible();
            changed.signalAll();
        } finally { lock.unlock(); }
        close.forEach(ConnectionPoolImpl::closeQuietly);
    }

    private void signalEligible() {
        if (stopped || waiters.isEmpty()) return;
        if (idle.isEmpty() && idle.size() + borrowed.size() + creating >= maxSize) return;
        waiters.get(0).condition.signal();
    }

    private void removeWaiter(Waiter waiter) {
        if (waiter != null && waiters.remove(waiter)) signalEligible();
    }

    private void reapLoop() {
        while (true) {
            List<RawConnection> close = new ArrayList<>();
            lock.lock();
            try {
                if (stopped) return;
                long now = System.nanoTime();
                while (idle.size() > minIdle) {
                    Entry oldest = idle.peekFirst();
                    long age = now - oldest.idleSince;
                    long limit = durationNanos(idleTimeout);
                    if (age < limit) break;
                    close.add(idle.removeFirst().raw);
                    now = System.nanoTime();
                }
                if (close.isEmpty()) {
                    if (idle.size() > minIdle) {
                        long remaining = durationNanos(idleTimeout) - (System.nanoTime() - idle.peekFirst().idleSince);
                        try { reaperChanged.awaitNanos(Math.max(1, Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(500)))); }
                        catch (InterruptedException ignored) { if (stopped) return; }
                    } else {
                        try { reaperChanged.await(); }
                        catch (InterruptedException ignored) { if (stopped) return; }
                    }
                }
            } finally { lock.unlock(); }
            close.forEach(ConnectionPoolImpl::closeQuietly);
        }
    }

    private static long durationNanos(Duration duration) {
        try { return duration.toNanos(); } catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }

    private static void closeQuietly(RawConnection raw) {
        try { raw.close(); } catch (Throwable ignored) { }
    }

    private final class Handle implements PooledConnection {
        private final Entry entry;
        private boolean returned;
        private Handle(Entry entry) { this.entry = entry; }
        @Override public RawConnection raw() {
            lock.lock();
            try {
                if (returned || entry.closed || !borrowed.containsKey(entry))
                    throw new IllegalStateException("Соединение больше не выдано");
                return entry.raw;
            } finally { lock.unlock(); }
        }
        @Override public void close() { returnEntry(entry, this); }
    }

    private static final class Entry {
        final RawConnection raw;
        long idleSince;
        volatile boolean closed;
        Entry(RawConnection raw) { this.raw = raw; }
    }

    private static final class Waiter {
        final Condition condition;
        Waiter(Condition condition) { this.condition = condition; }
    }
}
