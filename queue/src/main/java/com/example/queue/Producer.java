package com.example.queue;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Producer implementation for the distributed queue.
 * Produces items and adds them to the queue.
 */
public class Producer implements Runnable {
    private final DistributedQueue queue;
    private final String producerId;
    private final long delay;
    private final AtomicBoolean running;
    private final Random random;

    /**
     * Creates a new producer.
     *
     * @param zkServers Comma-separated list of ZooKeeper servers
     * @param queueName Name of the queue
     * @param delay Delay between producing items (in milliseconds)
     * @throws IOException If ZooKeeper connection fails
     * @throws InterruptedException If the connection process is interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    public Producer(String zkServers, String queueName, long delay)
            throws IOException, InterruptedException, KeeperException {
        this.queue = new DistributedQueue(zkServers, queueName);
        this.producerId = "producer-" + UUID.randomUUID().toString().substring(0, 8);
        this.delay = delay;
        this.running = new AtomicBoolean(true);
        this.random = new Random();
    }

    /**
     * Produces an item and adds it to the queue.
     *
     * @return The produced item
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public String produce() throws KeeperException, InterruptedException {
        String item = producerId + "-item-" + System.currentTimeMillis();
        queue.enqueue(item);
        return item;
    }

    /**
     * Stops the producer.
     */
    public void stop() {
        running.set(false);
    }

    /**
     * Closes the producer and its queue connection.
     *
     * @throws InterruptedException If the close operation is interrupted
     */
    public void close() throws InterruptedException {
        queue.close();
    }

    @Override
    public void run() {
        try {
            System.out.println(producerId + " started.");

            while (running.get()) {
                try {
                    String item = produce();
                    System.out.println(producerId + " produced: " + item);

                    // Make sure the delay is positive to avoid IllegalArgumentException
                    long actualDelay = delay;
                    if (delay > 1) {
                        // Only add randomization if delay is large enough to avoid negative values
                        actualDelay = delay + random.nextInt((int) Math.max(1, delay / 2));
                    }

                    Thread.sleep(actualDelay);
                } catch (KeeperException e) {
                    System.err.println(producerId + " error: " + e.getMessage());
                    // Exponential backoff on error
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            System.out.println(producerId + " interrupted.");
        } finally {
            try {
                close();
                System.out.println(producerId + " stopped.");
            } catch (InterruptedException e) {
                System.err.println(producerId + " error during close: " + e.getMessage());
            }
        }
    }

}