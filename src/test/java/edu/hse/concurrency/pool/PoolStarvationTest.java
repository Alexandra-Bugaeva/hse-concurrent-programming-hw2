package edu.hse.concurrency.pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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

@Timeout(15)
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
            printThreadDump();
        } finally {
            workers.shutdownNow();
            pool.shutdown(Duration.ZERO);
            workers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void printThreadDump() throws Exception {
        String jpsExecutable = System.getProperty("os.name").startsWith("Windows") ? "jps.exe" : "jps";
        Path jps = Path.of(System.getProperty("java.home"), "bin", jpsExecutable);
        Process listing = new ProcessBuilder(jps.toString(), "-l").redirectErrorStream(true).start();
        String processes = new String(listing.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, listing.waitFor(), processes);
        String currentPid = Long.toString(ProcessHandle.current().pid());
        String pid = processes.lines()
                .filter(line -> line.startsWith(currentPid + " "))
                .map(line -> line.substring(0, line.indexOf(' ')))
                .findFirst()
                .orElseThrow(() -> new AssertionError("jps -l did not list the test process:\n" + processes));

        String jcmdExecutable = System.getProperty("os.name").startsWith("Windows") ? "jcmd.exe" : "jcmd";
        Path jcmd = Path.of(System.getProperty("java.home"), "bin", jcmdExecutable);
        Process process = new ProcessBuilder(jcmd.toString(), pid, "Thread.print")
                .redirectErrorStream(true).start();
        String dump = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.waitFor(), dump);
        assertTrue(dump.contains("ConnectionPoolImpl.borrow"), dump);
        System.out.println("jps -l process:\n" + processes.lines()
                .filter(line -> line.startsWith(pid + " ")).findFirst().orElseThrow());
        System.out.println("Actual jcmd " + pid + " Thread.print output:\n" + relevantThread(dump));
    }

    private static String relevantThread(String dump) {
        String[] lines = dump.split("\\R");
        StringBuilder thread = new StringBuilder();
        String header = "";
        for (String line : lines) {
            if (line.startsWith("\"")) {
                if (header.startsWith("\"pool-") && thread.indexOf("ConnectionPoolImpl.borrow") >= 0)
                    return thread.toString();
                header = line;
                thread.setLength(0);
            }
            if (!header.isEmpty()) thread.append(line).append(System.lineSeparator());
        }
        if (header.startsWith("\"pool-") && thread.indexOf("ConnectionPoolImpl.borrow") >= 0)
            return thread.toString();
        throw new AssertionError("Could not find a starved worker in jcmd output");
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
