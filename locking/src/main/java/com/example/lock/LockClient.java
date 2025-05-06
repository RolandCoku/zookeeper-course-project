package com.example.lock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An example demonstrating the use of ZkDistributedLock
 * to access a shared resource from multiple clients.
 */
public class LockClient {

    // A simple class representing the shared resource with additional synchronization
    static class SharedResource {
        private volatile int value = 0;
        private final Object lock = new Object();

        // Synchronized method to increment the value atomically
        public void incrementValue() {
            synchronized (lock) {
                int current = value;
                // Simulate a time-consuming operation
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                value = current + 1;
                System.out.println(Thread.currentThread().getName() + " changed value to: " + value);
            }
        }

        public int getValue() {
            synchronized (lock) {
                return value;
            }
        }
    }

    static class Worker implements Runnable {
        private final String zkHosts;
        private final String resourceName;
        private final SharedResource resource;
        private final int incrementTimes;
        private final long lockTimeoutMs = 30000; // 30 seconds timeout for lock acquisition

        public Worker(String zkHosts, String resourceName, SharedResource resource, int incrementTimes) {
            this.zkHosts = zkHosts;
            this.resourceName = resourceName;
            this.resource = resource;
            this.incrementTimes = incrementTimes;
        }

        @Override
        public void run() {
            ZkDistributedLock lock = null;

            try {
                lock = new ZkDistributedLock(zkHosts, resourceName);

                for (int i = 0; i < incrementTimes; i++) {
                    System.out.println(Thread.currentThread().getName() + " attempting to acquire lock...");

                    boolean acquired = false;
                    try {
                        // Attempt to acquire the lock with timeout
                        acquired = lock.acquire(lockTimeoutMs);

                        if (acquired) {
                            System.out.println(Thread.currentThread().getName() + " acquired the lock.");

                            // Operate on shared resource
                            resource.incrementValue();

                            // Simulate time-consuming work
                            Thread.sleep((long) (Math.random() * 500));
                        } else {
                            System.out.println(Thread.currentThread().getName() + " could not acquire the lock within timeout.");
                        }
                    } catch (Exception e) {
                        System.err.println(Thread.currentThread().getName() + " encountered error during lock acquisition: " + e.getMessage());
                        e.printStackTrace();

                        // Wait a bit before retrying with a new lock instance
                        Thread.sleep(1000);

                        // Close the existing lock and create a new one
                        try {
                            lock.close();
                            lock = new ZkDistributedLock(zkHosts, resourceName);
                        } catch (Exception recreateErr) {
                            System.err.println("Error creating new lock instance: " + recreateErr.getMessage());
                        }
                    } finally {
                        if (acquired) {
                            try {
                                // Release the lock
                                lock.release();
                                System.out.println(Thread.currentThread().getName() + " released the lock.");
                            } catch (Exception e) {
                                System.err.println(Thread.currentThread().getName() + " encountered error during lock release: " + e.getMessage());
                            }
                        }
                    }

                    // Wait a bit before retrying
                    Thread.sleep((long) (Math.random() * 300) + 200);
                }
            } catch (Exception e) {
                System.err.println(Thread.currentThread().getName() + " encountered fatal error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // Ensure the lock is closed
                if (lock != null) {
                    try {
                        lock.close();
                    } catch (Exception e) {
                        System.err.println("Error closing lock: " + e.getMessage());
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        // ZooKeeper's connection configuration - with all cluster nodes
        String zkHosts = "localhost:2181,localhost:2182,localhost:2183,localhost:2184,localhost:2185";
        String resourceName = "shared-counter-lock";

        // The shared resource to be accessed
        SharedResource sharedResource = new SharedResource();

        // Number of clients that will compete for the lock
        int numClients = 5;

        // How many times each client will increment the value of the shared resource
        int incrementsPerClient = 2;

        // Create a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(numClients);

        System.out.println("Starting " + numClients + " clients, each will increment the value " + incrementsPerClient + " times.");

        // Submit jobs to the pool
        for (int i = 0; i < numClients; i++) {
            Worker worker = new Worker(zkHosts, resourceName, sharedResource, incrementsPerClient);
            executor.submit(worker);
        }

        // Shutdown the pool gracefully
        executor.shutdown();

        // Wait for all jobs to complete, but with timeout
        try {
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                System.out.println("Some threads did not finish within the timeout.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted waiting for threads to complete.");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Display a final result
        System.out.println("\nAll operations completed.");
        System.out.println("Final value of the shared resource: " + sharedResource.getValue());
        System.out.println("Expected value: " + (numClients * incrementsPerClient));

        if (sharedResource.getValue() == numClients * incrementsPerClient) {
            System.out.println("SUCCESS: The distributed lock worked properly!");
        } else {
            System.out.println("ERROR: The distributed lock did not work as expected.");
        }
    }
}