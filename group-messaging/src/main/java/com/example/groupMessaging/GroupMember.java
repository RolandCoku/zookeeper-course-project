package com.example.groupMessaging;

import com.example.groupMessaging.interfaces.MembershipListener;
import com.example.groupMessaging.interfaces.MessageListener;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Represents a member in a group with broadcasting capabilities
 */
public class GroupMember implements Watcher {
    private static final String GROUPS_PATH = "/groups";
    private static final String BROADCASTS_PATH = "/broadcasts";
    private static final int SESSION_TIMEOUT = 5000;

    private final ZooKeeper zooKeeper;
    private final String memberId;
    private final Map<String, Set<String>> knownMembers = new HashMap<>();
    private final Map<String, Integer> lastProcessedMessage = new HashMap<>();
    private final MessageListener messageListener;
    private final MembershipListener membershipListener;

    /**
     * Creates a new group member
     *
     * @param zkServers ZooKeeper server connection string
     * @param memberId Unique ID for this member
     * @param messageListener Listener for broadcast messages
     * @param membershipListener Listener for group membership changes
     * @throws IOException If ZooKeeper connection fails
     * @throws InterruptedException If connection is interrupted
     */
    public GroupMember(String zkServers, String memberId,
                       MessageListener messageListener,
                       MembershipListener membershipListener) throws IOException, InterruptedException, KeeperException {
        this.memberId = memberId;
        this.messageListener = messageListener;
        this.membershipListener = membershipListener;

        // Connect to ZooKeeper
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(zkServers, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });

        boolean connected = connectedSignal.await(10, java.util.concurrent.TimeUnit.SECONDS);
        if (!connected) {
            throw new IOException("Failed to connect to ZooKeeper");
        }

        // Create base paths if they don't exist
        createPath(GROUPS_PATH);
        createPath(BROADCASTS_PATH);
    }

    /**
     * Joins a group
     *
     * @param groupId ID of the group to join
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    public void joinGroup(String groupId) throws KeeperException, InterruptedException {
        // Create group path if it doesn't exist
        String groupPath = GROUPS_PATH + "/" + groupId;
        createPath(groupPath);

        // Create broadcast path if it doesn't exist
        String broadcastPath = BROADCASTS_PATH + "/" + groupId;
        createPath(broadcastPath);

        // Create ephemeral sequential node for this member
        byte[] data = memberId.getBytes(StandardCharsets.UTF_8);
        String memberPath = zooKeeper.create(
                groupPath + "/member-",
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Joined group: " + groupId + " as " + memberPath);

        // Watch for membership changes
        watchGroupMembers(groupId);

        // Watch for broadcast messages
        watchBroadcasts(groupId);
    }

    /**
     * Leaves a group
     *
     * @param groupId ID of the group to leave
     */
    public void leaveGroup(String groupId) {
        // Member node will be automatically deleted when the session ends
        System.out.println("Left group: " + groupId);
    }

    /**
     * Sends a broadcast message to a group
     *
     * @param groupId Group to send the message to
     * @param message Message content
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    public void broadcast(String groupId, String message) throws KeeperException, InterruptedException {
        String broadcastPath = BROADCASTS_PATH + "/" + groupId;

        // Message format: sender ID + message content
        String fullMessage = memberId + ":" + message;
        byte[] data = fullMessage.getBytes(StandardCharsets.UTF_8);

        // Create sequential node for the message
        String msgPath = zooKeeper.create(
                broadcastPath + "/msg-",
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Broadcast message: " + msgPath);
    }

    /**
     * Closes the ZooKeeper connection
     *
     * @throws InterruptedException If operation is interrupted
     */
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    /**
     * Creates a path in ZooKeeper if it doesn't exist
     *
     * @param path Path to create
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private void createPath(String path) throws KeeperException, InterruptedException {
        try {
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Node already exists, ignore
        }
    }

    /**
     * Watches for membership changes in a group
     *
     * @param groupId Group to watch
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private void watchGroupMembers(String groupId) throws KeeperException, InterruptedException {
        String groupPath = GROUPS_PATH + "/" + groupId;

        // Get current members and set watch
        List<String> members = zooKeeper.getChildren(groupPath, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    // Process membership changes and set new watch
                    processGroupMembershipChanges(groupId);
                } catch (Exception e) {
                    System.err.println("Error processing membership changes: " + e.getMessage());
                }
            }
        });

        // Initialize known members set if not present
        knownMembers.putIfAbsent(groupId, new HashSet<>());
        Set<String> currentlyKnownMembers = knownMembers.get(groupId);

        // Process current members
        for (String member : members) {
            if (!currentlyKnownMembers.contains(member)) {
                // New member
                currentlyKnownMembers.add(member);

                // Get member ID from znode data
                String memberPath = groupPath + "/" + member;
                byte[] data = zooKeeper.getData(memberPath, false, null);
                String id = new String(data, StandardCharsets.UTF_8);

                membershipListener.onMemberJoined(groupId, id);
            }
        }

        // Check for members that have left
        Iterator<String> it = currentlyKnownMembers.iterator();
        while (it.hasNext()) {
            String member = it.next();
            if (!members.contains(member)) {
                // Member has left
                it.remove();

                // We don't have the member ID anymore, but we can use the znode name
                membershipListener.onMemberLeft(groupId, member);
            }
        }
    }

    /**
     * Process membership changes and reset watch
     *
     * @param groupId Group to process
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private void processGroupMembershipChanges(String groupId) throws KeeperException, InterruptedException {
        String groupPath = GROUPS_PATH + "/" + groupId;

        // Get current members and set watch again
        List<String> members = zooKeeper.getChildren(groupPath, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    processGroupMembershipChanges(groupId);
                } catch (Exception e) {
                    System.err.println("Error processing membership changes: " + e.getMessage());
                }
            }
        });

        Set<String> currentlyKnownMembers = knownMembers.computeIfAbsent(groupId, k -> new HashSet<>());

        // Find new members
        for (String member : members) {
            if (!currentlyKnownMembers.contains(member)) {
                // New member
                currentlyKnownMembers.add(member);

                // Get member ID from znode data
                String memberPath = groupPath + "/" + member;
                try {
                    byte[] data = zooKeeper.getData(memberPath, false, null);
                    String id = new String(data, StandardCharsets.UTF_8);

                    membershipListener.onMemberJoined(groupId, id);
                } catch (KeeperException.NoNodeException e) {
                    // Member already left, ignore
                }
            }
        }

        // Find members that have left
        Iterator<String> it = currentlyKnownMembers.iterator();
        while (it.hasNext()) {
            String member = it.next();
            if (!members.contains(member)) {
                // Member has left
                it.remove();
                membershipListener.onMemberLeft(groupId, member);
            }
        }
    }

    /**
     * Watches for broadcast messages in a group
     *
     * @param groupId Group to watch
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private void watchBroadcasts(String groupId) throws KeeperException, InterruptedException {
        String broadcastPath = BROADCASTS_PATH + "/" + groupId;

        // Initialize last processed message ID if not present
        lastProcessedMessage.putIfAbsent(groupId, 0);

        // Get current broadcasts and set watch
        List<String> broadcasts = zooKeeper.getChildren(broadcastPath, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    // Process new messages and set new watch
                    processBroadcasts(groupId);
                } catch (Exception e) {
                    System.err.println("Error processing broadcasts: " + e.getMessage());
                }
            }
        });

        // Process current broadcasts
        processMessages(groupId, broadcasts);
    }

    /**
     * Process broadcasts and reset watch
     *
     * @param groupId Group to process
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private void processBroadcasts(String groupId) throws KeeperException, InterruptedException {
        String broadcastPath = BROADCASTS_PATH + "/" + groupId;

        // Get current broadcasts and set watch again
        List<String> broadcasts = zooKeeper.getChildren(broadcastPath, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    processBroadcasts(groupId);
                } catch (Exception e) {
                    System.err.println("Error processing broadcasts: " + e.getMessage());
                }
            }
        });

        processMessages(groupId, broadcasts);
    }

    /**
     * Process broadcast messages
     *
     * @param groupId Group to process
     * @param messages List of message znodes
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private void processMessages(String groupId, List<String> messages) throws KeeperException, InterruptedException {
        String broadcastPath = BROADCASTS_PATH + "/" + groupId;
        int lastProcessed = lastProcessedMessage.get(groupId);

        // Sort messages by sequence number
        Collections.sort(messages);

        for (String message : messages) {
            // Extract sequence number from node name
            int seqNum = extractSequenceNumber(message);

            // Skip already processed messages
            if (seqNum <= lastProcessed) {
                continue;
            }

            // Get message data
            try {
                String messagePath = broadcastPath + "/" + message;
                byte[] data = zooKeeper.getData(messagePath, false, null);
                String fullMessage = new String(data, StandardCharsets.UTF_8);

                // Parse sender ID and content
                int separatorIndex = fullMessage.indexOf(':');
                if (separatorIndex > 0) {
                    String senderId = fullMessage.substring(0, separatorIndex);
                    String content = fullMessage.substring(separatorIndex + 1);

                    // Don't deliver our own messages
                    if (!senderId.equals(memberId)) {
                        messageListener.onMessageReceived(groupId, content, senderId);
                    }
                }

                // Update last processed message
                lastProcessedMessage.put(groupId, seqNum);
            } catch (KeeperException.NoNodeException e) {
                // Message was deleted, skip
            }
        }
    }

    /**
     * Extracts sequence number from znode name
     *
     * @param nodeName Name of the znode
     * @return Sequence number
     */
    private int extractSequenceNumber(String nodeName) {
        int dashIndex = nodeName.lastIndexOf('-');
        if (dashIndex > 0 && dashIndex < nodeName.length() - 1) {
            try {
                return Integer.parseInt(nodeName.substring(dashIndex + 1));
            } catch (NumberFormatException e) {
                // Invalid format, return 0
                return 0;
            }
        }
        return 0;
    }

    @Override
    public void process(WatchedEvent event) {
        // General watcher, not used for specific watches
    }
}