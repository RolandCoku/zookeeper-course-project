package com.example.queue;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer implementation for the distributed queue.
 * Consumes items from the queue and processes them.
 */
public class Consumer implements Runnable {
    private final DistributedQueue queue;
    private final String consumerId;
    private final long processingTime;
    private final AtomicBoolean running;
    private final Random random;

    /**
     * Creates a new consumer.
     *
     * @param zkServers Comma-separated list of ZooKeeper servers
     * @param queueName Name of the queue
     * @param processingTime Time to process each item (in milliseconds)
     * @throws IOException If ZooKeeper connection fails
     * @throws InterruptedException If the connection process is interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    public Consumer(String zkServers, String queueName, long processingTime)
            throws IOException, InterruptedException, KeeperException {
        this.queue = new DistributedQueue(zkServers, queueName);
        this.consumerId = "consumer-" + UUID.randomUUID().toString().substring(0, 8);
        this.processingTime = processingTime;
        this.running = new AtomicBoolean(true);
        this.random = new Random();
    }

    /**
     * Consumes and processes an item from the queue.
     *
     * @return The consumed item, or null if the queue is empty
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public String consume() throws KeeperException, InterruptedException {
        String item = queue.dequeueString();

        if (item != null) {
            // Simulate processing time with some randomness
            long actualProcessingTime = processingTime + random.nextInt((int) (processingTime / 2));
            Thread.sleep(actualProcessingTime);
        }

        return item;
    }

    /**
     * Stops the consumer.
     */
    public void stop() {
        running.set(false);
    }

    /**
     * Closes the consumer and its queue connection.
     *
     * @throws InterruptedException If the close operation is interrupted
     */
    public void close() throws InterruptedException {
        queue.close();
    }

    @Override
    public void run() {
        try {
            System.out.println(consumerId + " started.");

            while (running.get()) {
                try {
                    String item = consume();

                    if (item != null) {
                        System.out.println(consumerId + " consumed: " + item);
                    } else {
                        // If the queue is empty, wait a bit before trying again
                        Thread.sleep(500);
                    }
                } catch (KeeperException e) {
                    System.err.println(consumerId + " error: " + e.getMessage());
                    // Exponential backoff on error
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            System.out.println(consumerId + " interrupted.");
        } finally {
            try {
                close();
                System.out.println(consumerId + " stopped.");
            } catch (InterruptedException e) {
                System.err.println(consumerId + " error during close: " + e.getMessage());
            }
        }
    }

}