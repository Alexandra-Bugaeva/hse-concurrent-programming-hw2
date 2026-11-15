package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(20)
class PoolStarvationTest {
    private static final int T = 4;

    @Test
    void maxSizeEqualToThreadCountStarves() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, T, Duration.ofMinutes(1), true), () -> () -> { });
        ExecutorService workers = Executors.newFixedThreadPool(T);
        CountDownLatch holdingFirst = new CountDownLatch(T);
        CountDownLatch allTryingSecond = new CountDownLatch(T);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < T; i++) futures.add(workers.submit(() -> {
                try (PooledConnection first = pool.borrow(Duration.ofSeconds(5))) {
                    holdingFirst.countDown();
                    if (!holdingFirst.await(5, TimeUnit.SECONDS)) throw new AssertionError("barrier timeout");
                    allTryingSecond.countDown();
                    try (PooledConnection second = pool.borrow(Duration.ofMinutes(1))) { }
                }
                return null;
            }));
            assertTrue(allTryingSecond.await(5, TimeUnit.SECONDS));
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (pool.stats().waiting() != T && System.nanoTime() < until)
                TimeUnit.MILLISECONDS.sleep(1);
            assertEquals(T, pool.stats().waiting());
            for (Future<?> future : futures) assertFalse(future.isDone());
            System.out.println("Starvation test PID=" + ProcessHandle.current().pid()
                    + "; capture it with jcmd <pid> Thread.print now.");
            new CountDownLatch(1).await(10, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
            pool.shutdown(Duration.ZERO);
            workers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void minimumCapacityLetsAllThreadsFinish() throws Exception {
        int connectionsPerThread = 2;
        int minimum = T * (connectionsPerThread - 1) + 1;
        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, minimum, Duration.ofMinutes(1), true), () -> () -> { });
        ExecutorService workers = Executors.newFixedThreadPool(T);
        CountDownLatch holdingFirst = new CountDownLatch(T);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < T; i++) futures.add(workers.submit(() -> {
                try (PooledConnection first = pool.borrow(Duration.ofSeconds(5))) {
                    holdingFirst.countDown();
                    if (!holdingFirst.await(5, TimeUnit.SECONDS)) throw new AssertionError("barrier timeout");
                    try (PooledConnection second = pool.borrow(Duration.ofSeconds(5))) { }
                }
                return null;
            }));
            for (Future<?> future : futures) future.get(10, TimeUnit.SECONDS);
            assertEquals(0, pool.stats().borrowed());
        } finally {
            workers.shutdownNow();
            pool.shutdown(Duration.ZERO);
            workers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
