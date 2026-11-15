package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class ConnectionPoolShutdownTest {
    @Test
    void repeatedShutdownDoesNotShortenFirstGrace() throws Exception {
        ConnectionPool pool = new ConnectionPoolImpl(
                new PoolConfig(0, 1, Duration.ofMinutes(1), true), () -> () -> { });
        PooledConnection connection = pool.borrow(Duration.ZERO);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> firstShutdown = executor.submit(() -> {
            pool.shutdown(Duration.ofSeconds(5));
            return null;
        });
        try {
            awaitStopped(pool);

            pool.shutdown(Duration.ZERO);

            assertFalse(firstShutdown.isDone(), "the first shutdown must still be waiting for its grace");
            assertDoesNotThrow(connection::raw, "the second shutdown must not force-close the connection");
            connection.close();
            firstShutdown.get(2, TimeUnit.SECONDS);
        } finally {
            connection.close();
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private static void awaitStopped(ConnectionPool pool) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            try {
                pool.borrow(Duration.ZERO).close();
            } catch (IllegalStateException stopped) {
                return;
            } catch (PoolTimeoutException stillRunning) {
                TimeUnit.MILLISECONDS.sleep(1);
            }
        }
        assertThrows(IllegalStateException.class, () -> pool.borrow(Duration.ZERO));
        assertTrue(false, "shutdown did not start in time");
    }
}
