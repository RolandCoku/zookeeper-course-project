/**
 * ZooKeeper-based Distributed Queue
 * This implementation provides a fault-tolerant distributed queue using Apache ZooKeeper.
 * It supports multiple producers and consumers with concurrent operations.
 */

package com.example.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Main distributed queue implementation
 */
public class DistributedQueue {
    private static final String QUEUE_ROOT = "/distributed-queue";
    private static final String QUEUE_NODE = "/queue-";
    private static final int SESSION_TIMEOUT = 5000;

    private final ZooKeeper zooKeeper;
    private final String queuePath;

    /**
     * Creates a new distributed queue.
     *
     * @param zkServers Comma-separated list of ZooKeeper servers (e.g., "localhost:2181,localhost:2182")
     * @param queueName Name of the queue
     * @throws IOException If ZooKeeper connection fails
     * @throws InterruptedException If the connection process is interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    public DistributedQueue(String zkServers, String queueName) throws IOException, InterruptedException, KeeperException {
        this.queuePath = QUEUE_ROOT + "/" + queueName;

        // Use CountDownLatch to wait for ZooKeeper connection
        final CountDownLatch connectedSignal = new CountDownLatch(1);

        // Connect to ZooKeeper with a watcher for connection status
        this.zooKeeper = new ZooKeeper(zkServers, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });

        // Wait for connection to establish
        boolean connected = connectedSignal.await(10, TimeUnit.SECONDS);
        if (!connected) {
            throw new IOException("Failed to connect to ZooKeeper within timeout");
        }

        // Create queue root if it doesn't exist
        createNodeIfNotExists(QUEUE_ROOT, new byte[0]);

        // Create queue-specific node if it doesn't exist
        createNodeIfNotExists(queuePath, new byte[0]);
    }

    /**
     * Utility method to create a ZooKeeper node if it doesn't exist.
     */
    private void createNodeIfNotExists(String path, byte[] data) throws KeeperException, InterruptedException {
        try {
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // Node already exists, ignore
        }
    }

    /**
     * Adds an item to the queue.
     *
     * @param data The data to enqueue
     * @return Path of the created node
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public String enqueue(byte[] data) throws KeeperException, InterruptedException {
        // Create a sequential node under the queue path
        return zooKeeper.create(queuePath + QUEUE_NODE, data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    /**
     * Adds a string item to the queue.
     *
     * @param item The string to enqueue
     * @return Path of the created node
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public String enqueue(String item) throws KeeperException, InterruptedException {
        return enqueue(item.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Dequeues an item from the queue.
     * Uses optimistic locking to handle concurrent consumers.
     *
     * @return The data of the dequeued item, or null if the queue is empty
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public byte[] dequeue() throws KeeperException, InterruptedException {
        while (true) {
            // Get all children of the queue path, sorted by sequence number
            List<String> children = zooKeeper.getChildren(queuePath, false);

            if (children.isEmpty()) {
                return null;  // Queue is empty
            }

            // Sort children to get the oldest item
            Collections.sort(children);
            String firstNode = children.get(0);
            String nodePath = queuePath + "/" + firstNode;

            try {
                // Get the data before deleting
                byte[] data = zooKeeper.getData(nodePath, false, null);

                // Delete the node using optimistic locking with version check
                Stat stat = zooKeeper.exists(nodePath, false);
                if (stat != null) {
                    zooKeeper.delete(nodePath, stat.getVersion());
                    return data;
                }
            } catch (KeeperException.NoNodeException e) {
                // The Node was deleted by another consumer, try again
            }
        }
    }

    /**
     * Dequeues a string item from the queue.
     *
     * @return The string value of the dequeued item, or null if the queue is empty
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public String dequeueString() throws KeeperException, InterruptedException {
        byte[] data = dequeue();
        return data != null ? new String(data, StandardCharsets.UTF_8) : null;
    }

    /**
     * Peeks at the first item in the queue without removing it.
     *
     * @return The data of the first item, or null if the queue is empty
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public byte[] peek() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(queuePath, false);

        if (children.isEmpty()) {
            return null;  // Queue is empty
        }

        // Sort children to get the oldest item
        Collections.sort(children);
        String firstNode = children.get(0);
        String nodePath = queuePath + "/" + firstNode;

        try {
            return zooKeeper.getData(nodePath, false, null);
        } catch (KeeperException.NoNodeException e) {
            // The Node was deleted after we got the children list
            return null;
        }
    }

    /**
     * Returns the size of the queue.
     *
     * @return Number of items in the queue
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public int size() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(queuePath, false);
        return children.size();
    }

    /**
     * Closes the ZooKeeper connection.
     *
     * @throws InterruptedException If the close operation is interrupted
     */
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }
}