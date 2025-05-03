package com.example.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * An improved implementation of a distributed lock using ZooKeeper.
 * This class provides a robust mechanism where only one client can access
 * shared data at a time.
 */

public class ZkDistributedLock implements Watcher {

    private static final String LOCK_ROOT = "/locks";
    private static final String LOCK_NODE_PREFIX = "/lock-";

    // Longer timeout for connection
    private static final int SESSION_TIMEOUT = 60000; // 60 seconds

    private ZooKeeper zooKeeper;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private final String lockPath;
    private String lockNodePath;
    private String watchedNodePath;
    private final String zkHosts;

    // Lock used for synchronization
    private final Object lockObject = new Object();
    private boolean hasLock = false;

    /**
     * Constructor for ZkDistributedLock.
     *
     * @param zkHosts String containing host:port for ZooKeeper nodes (e.g. "localhost:2181,localhost:2182,localhost:2183")
     * @param lockName Name of the lock (will be used to create a unique path)
     * @throws IOException if connection to ZooKeeper fails
     * @throws InterruptedException if thread is interrupted during connection
     * @throws KeeperException if ZooKeeper operation fails
     */
    public ZkDistributedLock(String zkHosts, String lockName) throws IOException, InterruptedException, KeeperException {
        this.zkHosts = zkHosts;
        this.lockPath = LOCK_ROOT + "/" + lockName;

        // Establish connection to ZooKeeper with retry
        connect();
    }

    /**
     * Attempts to connect to ZooKeeper
     */
    private void connect() throws IOException, InterruptedException, KeeperException {
        try {
            System.out.println("Connecting to ZooKeeper");
            this.zooKeeper = new ZooKeeper(zkHosts, SESSION_TIMEOUT, this);

            // Wait to connect to ZooKeeper
            if (connectedSignal.await(15, TimeUnit.SECONDS)) {
                // Connection successfully established
                ensureRootExists();
            } else {
                throw new KeeperException.ConnectionLossException();
            }
        } catch (Exception e) {
            System.err.println("Error connecting to ZooKeeper: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Ensures the root lock node exists.
     */
    private void ensureRootExists() throws KeeperException, InterruptedException {
        try {
            // First check the general locks root
            Stat rootStat = zooKeeper.exists(LOCK_ROOT, false);
            if (rootStat == null) {
                try {
                    zooKeeper.create(LOCK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println("Created locks root: " + LOCK_ROOT);
                } catch (KeeperException.NodeExistsException e) {
                    // Someone else created it in the meantime, that's fine
                }
            }

            // Check the specific lock root
            Stat lockPathStat = zooKeeper.exists(lockPath, false);
            if (lockPathStat == null) {
                try {
                    zooKeeper.create(lockPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println("Created specific lock root: " + lockPath);
                } catch (KeeperException.NodeExistsException e) {
                    // Someone else created it in the meantime, that's fine
                }
            }
        } catch (Exception e) {
            System.err.println("Error ensuring lock root exists: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Attempts to acquire the lock with retry logic.
     * This method will block until the lock is available or an error occurs.
     *
     * @param timeoutMs Maximum time to wait in milliseconds
     * @return true if lock was successfully acquired, false in case of timeout
     * @throws KeeperException if ZooKeeper operation fails
     * @throws InterruptedException if thread is interrupted during wait
     */
    public boolean acquire(long timeoutMs) throws KeeperException, InterruptedException, IOException {
        long startTime = System.currentTimeMillis();
        boolean acquired = false;

        // Retry acquiring the lock for the specified time
        while (System.currentTimeMillis() - startTime < timeoutMs && !acquired) {
            try {
                acquired = tryAcquire();

                if (!acquired) {
                    // Wait a short time before trying again
                    Thread.sleep(200);
                }
            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
                System.err.println("Connection to ZooKeeper was lost during lock acquisition. Reconnecting...");

                // Clear previous states
                lockNodePath = null;
                watchedNodePath = null;

                try {
                    // Retry connection
                    connect();
                } catch (Exception reconnectError) {
                    System.err.println("Error during reconnection: " + reconnectError.getMessage());
                    throw reconnectError;
                }
            } catch (Exception e) {
                System.err.println("Unexpected error during lock acquisition: " + e.getMessage());
                throw e;
            }
        }

        return acquired;
    }

    /**
     * Attempts to acquire the lock in a single try.
     */
    private boolean tryAcquire() throws KeeperException, InterruptedException {
        synchronized (lockObject) {
            if (hasLock) {
                return true; // We already have the lock
            }

            // Create an ephemeral sequential node
            if (lockNodePath == null) {
                lockNodePath = zooKeeper.create(lockPath + LOCK_NODE_PREFIX, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println(Thread.currentThread().getName() + " created node: " + lockNodePath);
            }

            // Check if it's the node with the lowest sequence (first in line)
            boolean gotLock = checkLock();

            if (gotLock) {
                hasLock = true;
            }

            return gotLock;
        }
    }

    /**
     * Checks if we're first in line to get the lock.
     */
    private boolean checkLock() throws KeeperException, InterruptedException {
        try {
            // Get all nodes in the lock path
            List<String> children = zooKeeper.getChildren(lockPath, false);

            if (children.isEmpty()) {
                return false; // If there are no children, something went wrong
            }

            // Get just the node names without the full path
            Collections.sort(children);

            // Get our node name without the full path
            String shortNodePath = lockNodePath.substring(lockPath.length() + 1);

            // Check if our node still exists
            if (!children.contains(shortNodePath)) {
                System.out.println("WARNING: Our node no longer exists in the children list. Creating a new one.");
                lockNodePath = null;
                return false;
            }

            // If we're the first node, the lock is ours
            if (children.get(0).equals(shortNodePath)) {
                System.out.println(Thread.currentThread().getName() + " acquired lock: " + lockNodePath);
                return true;
            }

            // Otherwise, find the node that comes before us and set a watch on it
            int index = children.indexOf(shortNodePath);
            int watchIndex = index - 1;
            watchedNodePath = lockPath + "/" + children.get(watchIndex);

            // Set a watch to see when the node before us is deleted (lock is released)
            Stat stat = zooKeeper.exists(watchedNodePath, this);

            // If the node before us is already deleted, check again
            if (stat == null) {
                System.out.println("Watched node was deleted in the meantime. Rechecking.");
                return checkLock();
            }

            System.out.println(Thread.currentThread().getName() + " waiting for lock to be released. Position in queue: " + index);
            return false;
        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
            // Escalate these types of exceptions to be handled by the calling method
            throw e;
        } catch (Exception e) {
            System.err.println("Error checking lock: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Releases the lock by deleting the created node.
     *
     * @throws InterruptedException if thread is interrupted
     * @throws KeeperException if ZooKeeper operation fails
     */
    public void release() throws InterruptedException, KeeperException {
        synchronized (lockObject) {
            if (lockNodePath != null && hasLock) {
                try {
                    zooKeeper.delete(lockNodePath, -1);
                    System.out.println(Thread.currentThread().getName() + " released lock: " + lockNodePath);
                } catch (KeeperException.NoNodeException e) {
                    // Node is already deleted, can happen due to session expiration
                    System.out.println("Lock node was already deleted: " + lockNodePath);
                } finally {
                    lockNodePath = null;
                    hasLock = false;
                }
            }
        }
    }

    /**
     * Closes the connection to ZooKeeper.
     *
     * @throws InterruptedException if thread is interrupted
     */
    public void close() throws InterruptedException {
        try {
            // Release the lock if we still have it
            if (hasLock) {
                release();
            }
        } catch (Exception e) {
            System.err.println("Error releasing lock during close: " + e.getMessage());
        } finally {
            if (zooKeeper != null) {
                zooKeeper.close();
                zooKeeper = null;
            }
        }
    }

    /**
     * Implementation of the Watcher method from ZooKeeper Watcher interface.
     */
    @Override
    public void process(WatchedEvent event) {
        // Handle connection
        if (event.getState() == Event.KeeperState.SyncConnected) {
            System.out.println("Successfully connected to ZooKeeper.");
            connectedSignal.countDown();
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            System.out.println("Disconnected from ZooKeeper.");
        } else if (event.getState() == Event.KeeperState.Expired) {
            System.out.println("ZooKeeper session expired.");
        }

        // Handle watched node events
        if (event.getType() == Event.EventType.NodeDeleted &&
                event.getPath() != null &&
                event.getPath().equals(watchedNodePath)) {
            System.out.println("Watched node was deleted: " + watchedNodePath);
            try {
                // Node before us was deleted, restart check
                synchronized (lockObject) {
                    // Check if we can get the lock now
                    boolean acquired = checkLock();
                    if (acquired) {
                        hasLock = true;
                        synchronized (this) {
                            // Notify any thread waiting for the lock
                            this.notifyAll();
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error checking lock after node deletion: " + e.getMessage());
            }
        }
    }
}