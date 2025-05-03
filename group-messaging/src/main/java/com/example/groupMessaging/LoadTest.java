package com.example.groupMessaging;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Load testing application for the group broadcasting functionality
 */
public class LoadTest {

    private static final String DEFAULT_ZK_SERVERS = "localhost:2181";
    private static final int DEFAULT_GROUP_COUNT = 3;
    private static final int DEFAULT_MEMBER_COUNT = 5;
    private static final int DEFAULT_MESSAGE_COUNT = 100;
    private static final int DEFAULT_MESSAGE_DELAY_MS = 10;

    /**
     * Main entry point
     *
     * @param args Command-line arguments:
     *             [ZooKeeper servers] [Group count] [Member count per group] [Message count] [Message delay in ms]
     * @throws Exception If any error occurs
     */
    public static void main(String[] args) throws Exception {
        // Parse arguments
        String zkServers = args.length > 0 ? args[0] : DEFAULT_ZK_SERVERS;
        int groupCount = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_GROUP_COUNT;
        int memberCount = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_MEMBER_COUNT;
        int messageCount = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_MESSAGE_COUNT;
        int messageDelay = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_MESSAGE_DELAY_MS;

        System.out.println("Starting Load Test");
        System.out.println("ZooKeeper servers: " + zkServers);
        System.out.println("Group count: " + groupCount);
        System.out.println("Member count per group: " + memberCount);
        System.out.println("Message count: " + messageCount);
        System.out.println("Message delay (ms): " + messageDelay);

        // Create executor service for running member tasks
        ExecutorService executor = Executors.newFixedThreadPool(groupCount * memberCount);

        try {
            // Run load test
            runLoadTest(executor, zkServers, groupCount, memberCount, messageCount, messageDelay);
        } finally {
            // Shutdown executor
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
        }

        System.out.println("Load test completed");
    }

    /**
     * Runs the load test
     *
     * @param executor Executor service
     * @param zkServers ZooKeeper servers
     * @param groupCount Number of groups
     * @param memberCount Number of members per group
     * @param messageCount Number of messages to send
     * @param messageDelay Delay between messages in milliseconds
     * @throws InterruptedException If operation is interrupted
     */
    private static void runLoadTest(ExecutorService executor, String zkServers,
                                    int groupCount, int memberCount,
                                    int messageCount, int messageDelay)
            throws InterruptedException {

        // Create group IDs
        List<String> groupIds = new ArrayList<>();
        for (int i = 0; i < groupCount; i++) {
            groupIds.add("group-" + i);
        }

        // Create members and tasks
        List<MemberTask> tasks = new ArrayList<>();

        for (String groupId : groupIds) {
            // For each group
            for (int j = 0; j < memberCount; j++) {
                String memberId = "member-" + UUID.randomUUID().toString().substring(0, 8);

                // Create and register task
                MemberTask task = new MemberTask(zkServers, memberId, groupId, messageCount, messageDelay);
                tasks.add(task);

                // Submit task
                executor.submit(task);
            }
        }

        // Wait for all tasks to complete initialization
        System.out.println("Waiting for members to initialize...");
        for (MemberTask task : tasks) {
            task.waitForInitialization();
        }

        // Start sending messages
        System.out.println("Starting message broadcasting...");
        for (MemberTask task : tasks) {
            task.startBroadcasting();
        }

        // Wait for all tasks to complete
        System.out.println("Waiting for tasks to complete...");
        for (MemberTask task : tasks) {
            task.waitForCompletion();
        }

        // Print results
        int totalMessagesSent = 0;
        int totalMessagesReceived = 0;

        for (MemberTask task : tasks) {
            totalMessagesSent += task.getMessagesSent();
            totalMessagesReceived += task.getMessagesReceived();
        }

        System.out.println("Total messages sent: " + totalMessagesSent);
        System.out.println("Total messages received: " + totalMessagesReceived);
        System.out.println("Broadcast efficiency: " +
                String.format("%.2f%%", (double)totalMessagesReceived /
                        (totalMessagesSent * (memberCount - 1)) * 100));

        // Print per-group statistics
        System.out.println("\nPer-group statistics:");
        for (String groupId : groupIds) {
            int groupMessagesSent = 0;
            int groupMessagesReceived = 0;

            for (MemberTask task : tasks) {
                if (task.getGroupId().equals(groupId)) {
                    groupMessagesSent += task.getMessagesSent();
                    groupMessagesReceived += task.getMessagesReceived();
                }
            }

            System.out.println("Group " + groupId + ":");
            System.out.println("  Messages sent: " + groupMessagesSent);
            System.out.println("  Messages received: " + groupMessagesReceived);
            System.out.println("  Efficiency: " +
                    String.format("%.2f%%", (double)groupMessagesReceived /
                            (groupMessagesSent * (memberCount - 1)) * 100));
        }
    }

    /**
     * Task for running a group member
     */
    static class MemberTask implements Runnable, MessageListener, MembershipListener {
        private final String zkServers;
        private final String memberId;
        private final String groupId;
        private final int messageCount;
        private final int messageDelay;

        private final CountDownLatch initLatch = new CountDownLatch(1);
        private final CountDownLatch startLatch = new CountDownLatch(1);
        private final CountDownLatch completeLatch = new CountDownLatch(1);

        private GroupMember member;
        private int messagesSent = 0;
        private int messagesReceived = 0;
        private int membersJoined = 0;
        private int membersLeft = 0;

        /**
         * Creates a new member task
         *
         * @param zkServers ZooKeeper servers
         * @param memberId Member ID
         * @param groupId Group ID
         * @param messageCount Number of messages to send
         * @param messageDelay Delay between messages in milliseconds
         */
        public MemberTask(String zkServers, String memberId, String groupId, int messageCount, int messageDelay) {
            this.zkServers = zkServers;
            this.memberId = memberId;
            this.groupId = groupId;
            this.messageCount = messageCount;
            this.messageDelay = messageDelay;
        }

        @Override
        public void run() {
            try {
                // Create member
                member = new GroupMember(zkServers, memberId, this, this);

                // Join group
                member.joinGroup(groupId);

                // Signal initialization complete
                initLatch.countDown();

                // Wait for start signal
                startLatch.await();

                // Send messages
                for (int i = 0; i < messageCount; i++) {
                    String message = "Message " + i + " from " + memberId;
                    member.broadcast(groupId, message);
                    messagesSent++;

                    if (messageDelay > 0) {
                        Thread.sleep(messageDelay);
                    }
                }

                // Wait a bit for messages to be processed
                Thread.sleep(1000);

                // Leave group and cleanup
                member.leaveGroup(groupId);
                member.close();

                // Signal completion
                completeLatch.countDown();

            } catch (IOException | InterruptedException | KeeperException e) {
                System.err.println("Error in member task " + memberId + ": " + e.getMessage());
                initLatch.countDown();
                startLatch.countDown();
                completeLatch.countDown();
            }
        }

        @Override
        public void onMessageReceived(String groupId, String message, String senderId) {
            // Count received messages
            messagesReceived++;
        }

        @Override
        public void onMemberJoined(String groupId, String memberId) {
            // Count joined members
            membersJoined++;
        }

        @Override
        public void onMemberLeft(String groupId, String memberId) {
            // Count left members
            membersLeft++;
        }

        /**
         * Waits for initialization to complete
         *
         * @throws InterruptedException If operation is interrupted
         */
        public void waitForInitialization() throws InterruptedException {
            initLatch.await();
        }

        /**
         * Signals the task to start broadcasting
         */
        public void startBroadcasting() {
            startLatch.countDown();
        }

        /**
         * Waits for the task to complete
         *
         * @throws InterruptedException If operation is interrupted
         */
        public void waitForCompletion() throws InterruptedException {
            completeLatch.await();
        }

        /**
         * Gets the number of messages sent
         *
         * @return Number of messages sent
         */
        public int getMessagesSent() {
            return messagesSent;
        }

        /**
         * Gets the number of messages received
         *
         * @return Number of messages received
         */
        public int getMessagesReceived() {
            return messagesReceived;
        }

        /**
         * Gets the number of members joined
         *
         * @return Number of members joined
         */
        public int getMembersJoined() {
            return membersJoined;
        }

        /**
         * Gets the number of members left
         *
         * @return Number of members left
         */
        public int getMembersLeft() {
            return membersLeft;
        }

        /**
         * Gets the group ID
         *
         * @return Group ID
         */
        public String getGroupId() {
            return groupId;
        }
    }
}