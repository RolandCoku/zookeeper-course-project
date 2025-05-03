package com.example.queue;

import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import com.example.queue.utils.ClientHandler;

/**
 * Enhanced test application for the distributed queue that supports
 * running in multiple terminals.
 */
public class DistributedQueueTest {
    private static final String DEFAULT_ZK_SERVERS = "localhost:2181,localhost:2182,localhost:2183,localhost:2184,localhost:2185";
    private static final String DEFAULT_QUEUE_NAME = "test-queue";
    private static final int SERVER_PORT = 9876;

    /**
     * Main entry point for the test application.
     *
     * @param args Command-line arguments
     *             args[0]: Optional - ZooKeeper servers
     *             args[1]: Optional - Queue name
     *             args[2]: Optional - Mode ("server", "producer", or "consumer")
     *             args[3]: Optional - Additional parameters (count, delay, etc.)
     */
    public static void main(String[] args) {
        try {
            // Use command-line arguments or defaults
            String zkServers = args.length > 0 ? args[0] : DEFAULT_ZK_SERVERS;
            String queueName = args.length > 1 ? args[1] : DEFAULT_QUEUE_NAME;
            String mode = args.length > 2 ? args[2] : "server";

            System.out.println("Starting Distributed Queue Test");
            System.out.println("ZooKeeper servers: " + zkServers);
            System.out.println("Queue name: " + queueName);
            System.out.println("Mode: " + mode);

            switch (mode) {
                case "server":
                    runServerMode(zkServers, queueName);
                    break;
                case "producer":
                    if (args.length < 4) {
                        System.err.println("Producer mode requires delay parameter");
                        System.exit(1);
                    }
                    long delay = Long.parseLong(args[3]);
                    runProducerMode(zkServers, queueName, delay);
                    break;
                case "consumer":
                    if (args.length < 4) {
                        System.err.println("Consumer mode requires processing time parameter");
                        System.exit(1);
                    }
                    long processingTime = Long.parseLong(args[3]);
                    runConsumerMode(zkServers, queueName, processingTime);
                    break;
                default:
                    System.err.println("Unknown mode: " + mode);
                    System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Runs the server mode, showing the menu and managing the system.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void runServerMode(String zkServers, String queueName)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("=== Running in SERVER mode ===");

        // Start a server socket to communicate with clients
        ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
        List<ClientHandler> clientHandlers = new ArrayList<>();

        // Start a thread to accept client connections
        Thread connectionThread = getConnectionThread(serverSocket, clientHandlers);
        connectionThread.start();

        showTestMenu(zkServers, queueName, clientHandlers);

        // Clean up
        for (ClientHandler handler : clientHandlers) {
            handler.sendCommand("STOP");
            handler.close();
        }

        serverSocket.close();
    }

    private static Thread getConnectionThread(ServerSocket serverSocket, List<ClientHandler> clientHandlers) {
        Thread connectionThread = new Thread(() -> {
            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("New client connected: " + clientSocket);

                    ClientHandler handler = new ClientHandler(clientSocket);
                    clientHandlers.add(handler);
                    new Thread(handler).start();
                }
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    System.err.println("Server socket error: " + e.getMessage());
                }
            }
        });
        connectionThread.setDaemon(true);
        return connectionThread;
    }

    /**
     * Runs a producer in client mode.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @param delay Delay between producing items
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void runProducerMode(String zkServers, String queueName, long delay)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("=== Running in PRODUCER mode with delay " + delay + "ms ===");

        // Connect to the server
        Socket socket = new Socket("localhost", SERVER_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Send registration
        out.println("REGISTER|PRODUCER");

        // Start the producer
        Producer producer = new Producer(zkServers, queueName, delay);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        // Listen for commands
        String command;
        while ((command = in.readLine()) != null) {
            if (command.equals("STOP")) {
                producer.stop();
                break;
            }
        }

        // Clean up
        socket.close();
        System.exit(0);
    }

    /**
     * Runs a consumer in client mode.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @param processingTime Processing time per item
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void runConsumerMode(String zkServers, String queueName, long processingTime)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("=== Running in CONSUMER mode with processing time " + processingTime + "ms ===");

        // Connect to the server
        Socket socket = new Socket("localhost", SERVER_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Send registration
        out.println("REGISTER|CONSUMER");

        // Start the consumer
        Consumer consumer = new Consumer(zkServers, queueName, processingTime);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        // Listen for commands
        String command;
        while ((command = in.readLine()) != null) {
            if (command.equals("STOP")) {
                consumer.stop();
                break;
            }
        }

        // Clean up
        socket.close();
        System.exit(0);
    }

    /**
     * Shows the test menu and handles user input.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @param clientHandlers List of client handlers
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void showTestMenu(String zkServers, String queueName, List<ClientHandler> clientHandlers)
            throws IOException, InterruptedException, KeeperException {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== Distributed Queue Test Menu ===");
            System.out.println("1. Test basic queue operations");
            System.out.println("2. Start producer (in new terminal)");
            System.out.println("3. Start consumer (in new terminal)");
            System.out.println("4. Stop all producers");
            System.out.println("5. Stop all consumers");
            System.out.println("6. Simulate network partition");
            System.out.println("7. Simulate node failure");
            System.out.println("8. View queue status");
            System.out.println("9. View connected clients");
            System.out.println("10. Exit");
            System.out.print("Enter your choice: ");

            int choice = scanner.nextInt();
            scanner.nextLine();  // Consume newline

            switch (choice) {
                case 1:
                    testBasicQueueOperations(zkServers, queueName);
                    break;
                case 2:
                    System.out.print("Enter delay between items (ms): ");
                    long producerDelay = scanner.nextLong();
                    startProducerInNewTerminal(zkServers, queueName, producerDelay);
                    break;
                case 3:
                    System.out.print("Enter processing time per item (ms): ");
                    long processingTime = scanner.nextLong();
                    startConsumerInNewTerminal(zkServers, queueName, processingTime);
                    break;
                case 4:
                    stopAllClients(clientHandlers, "PRODUCER");
                    break;
                case 5:
                    stopAllClients(clientHandlers, "CONSUMER");
                    break;
                case 6:
                    simulateNetworkPartition(zkServers, queueName);
                    break;
                case 7:
                    simulateNodeFailure(zkServers, queueName);
                    break;
                case 8:
                    viewQueueStatus(zkServers, queueName);
                    break;
                case 9:
                    viewConnectedClients(clientHandlers);
                    break;
                case 10:
                    System.out.println("Exiting...");
                    return;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    /**
     * Starts a producer in a new terminal.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @param delay Delay between producing items
     * @throws IOException If I/O error occurs
     */
    private static void startProducerInNewTerminal(String zkServers, String queueName, long delay)
            throws IOException {
        System.out.println("\n=== Starting Producer in New Terminal ===");

        String javaCmd = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = DistributedQueueTest.class.getName();

        ProcessBuilder processBuilder = new ProcessBuilder(
                javaCmd, "-cp", classpath, className, zkServers, queueName, "producer", String.valueOf(delay));

        // Redirect output to new terminal based on OS
        String os = System.getProperty("os.name").toLowerCase();

        String s10 = javaCmd + " -cp " + classpath + " " + className + " " +
                zkServers + " " + queueName + " producer " + delay;

        if (buildProcess(processBuilder, os, s10)) return;
        System.out.println("Producer started in new terminal.");
    }

    private static boolean buildProcess(ProcessBuilder processBuilder, String os, String s10) throws IOException {
        if (os.contains("win")) {
            // Windows
            processBuilder.command("cmd", "/c", "start", "cmd", "/k",
                    s10);
        } else if (os.contains("nix") || os.contains("nux") || os.contains("mac")) {
            // Linux or Mac
            processBuilder.command("xterm", "-e",
                    s10);
        } else {
            System.err.println("Unsupported operating system: " + os);
            return true;
        }

        Process process = processBuilder.start();
        return false;
    }

    /**
     * Starts a consumer in a new terminal.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @param processingTime Processing time per item
     * @throws IOException If I/O error occurs
     */
    private static void startConsumerInNewTerminal(String zkServers, String queueName, long processingTime)
            throws IOException {
        System.out.println("\n=== Starting Consumer in New Terminal ===");

        String javaCmd = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = DistributedQueueTest.class.getName();

        ProcessBuilder processBuilder = new ProcessBuilder();

        // Redirect output to new terminal based on OS
        String os = System.getProperty("os.name").toLowerCase();

        String s = javaCmd + " -cp " + classpath + " " + className + " " +
                zkServers + " " + queueName + " consumer " + processingTime;
        if (buildProcess(processBuilder, os, s)) return;
        System.out.println("Consumer started in new terminal.");
    }

    /**
     * Stops all clients of a specific type.
     *
     * @param clientHandlers List of client handlers
     * @param clientType Type of clients to stop ("PRODUCER" or "CONSUMER")
     */
    private static void stopAllClients(List<ClientHandler> clientHandlers, String clientType) {
        System.out.println("\n=== Stopping All " + clientType + "s ===");

        int count = 0;
        for (ClientHandler handler : clientHandlers) {
            if (handler.getClientType().equals(clientType)) {
                handler.sendCommand("STOP");
                count++;
            }
        }

        System.out.println("Sent stop command to " + count + " " + clientType.toLowerCase() + "s");
    }

    /**
     * Views connected clients.
     *
     * @param clientHandlers List of client handlers
     */
    private static void viewConnectedClients(List<ClientHandler> clientHandlers) {
        System.out.println("\n=== Connected Clients ===");

        if (clientHandlers.isEmpty()) {
            System.out.println("No clients connected");
            return;
        }

        int producerCount = 0;
        int consumerCount = 0;

        for (ClientHandler handler : clientHandlers) {
            System.out.println("Client " + handler.getClientId() + ": " + handler.getClientType());

            if (handler.getClientType().equals("PRODUCER")) {
                producerCount++;
            } else if (handler.getClientType().equals("CONSUMER")) {
                consumerCount++;
            }
        }

        System.out.println("Total: " + clientHandlers.size() + " clients (" +
                producerCount + " producers, " + consumerCount + " consumers)");
    }

    /**
     * Tests basic queue operations (enqueue, dequeue, peek).
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void testBasicQueueOperations(String zkServers, String queueName)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("\n=== Testing Basic Queue Operations ===");

        try (Scanner scanner = new Scanner(System.in)) {
            DistributedQueue queue = new DistributedQueue(zkServers, queueName);

            while (true) {
                System.out.println("\n1. Enqueue item");
                System.out.println("2. Dequeue item");
                System.out.println("3. Peek at first item");
                System.out.println("4. Get queue size");
                System.out.println("5. Return to main menu");
                System.out.print("Enter your choice: ");

                int choice = scanner.nextInt();
                scanner.nextLine();  // Consume newline

                switch (choice) {
                    case 1:
                        System.out.print("Enter item to enqueue: ");
                        String item = scanner.nextLine();
                        String path = queue.enqueue(item);
                        System.out.println("Enqueued item: " + item);
                        System.out.println("Node path: " + path);
                        break;
                    case 2:
                        String dequeuedItem = queue.dequeueString();
                        if (dequeuedItem != null) {
                            System.out.println("Dequeued item: " + dequeuedItem);
                        } else {
                            System.out.println("Queue is empty");
                        }
                        break;
                    case 3:
                        byte[] peekedData = queue.peek();
                        if (peekedData != null) {
                            String peekedItem = new String(peekedData);
                            System.out.println("First item in queue: " + peekedItem);
                        } else {
                            System.out.println("Queue is empty");
                        }
                        break;
                    case 4:
                        int size = queue.size();
                        System.out.println("Queue size: " + size);
                        break;
                    case 5:
                        return;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            }
        }
    }

    /**
     * Simulates a network partition.
     * This is done by creating a temporary ZooKeeper client with a session timeout.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void simulateNetworkPartition(String zkServers, String queueName)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("\n=== Simulating Network Partition ===");
        System.out.println("Creating a temporary client and disconnecting it...");

        // Create a temporary queue client
        DistributedQueue tempQueue = new DistributedQueue(zkServers, queueName);

        // Add some items
        System.out.println("Adding items before partition...");
        for (int i = 0; i < 5; i++) {
            String item = "partition-test-" + i;
            tempQueue.enqueue(item);
            System.out.println("Added: " + item);
        }

        System.out.println("Simulating network partition...");
        System.out.println("In a real environment, you would use iptables or similar to block ZooKeeper traffic");
        System.out.println("For this test, we'll just close the connection and wait for session timeout");

        // Close the connection to simulate partition
        tempQueue.close();

        // Wait for session timeout
        System.out.println("Waiting for session timeout...");
        Thread.sleep(10000);  // Wait longer than the session timeout

        System.out.println("Reconnecting after partition...");
        DistributedQueue reconnectedQueue = new DistributedQueue(zkServers, queueName);

        // Try to dequeue items
        System.out.println("Trying to dequeue items after reconnection...");
        for (int i = 0; i < 5; i++) {
            String item = reconnectedQueue.dequeueString();
            System.out.println("Dequeued: " + item);
        }

        reconnectedQueue.close();
        System.out.println("Network partition test completed");
    }

    /**
     * Simulates a node failure.
     * This is done by creating a temporary ZooKeeper client and killing its session.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void simulateNodeFailure(String zkServers, String queueName)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("\n=== Simulating Node Failure ===");

        // Create a temporary queue client for the "failing" node
        DistributedQueue nodeQueue = new DistributedQueue(zkServers, queueName);

        // Add some items from the "failing" node
        System.out.println("Adding items from the node that will fail...");
        for (int i = 0; i < 5; i++) {
            String item = "node-failure-test-" + i;
            nodeQueue.enqueue(item);
            System.out.println("Added: " + item);
        }

        // Simulate node failure by forcibly closing the connection
        System.out.println("Simulating node failure...");
        nodeQueue.close();

        // Wait a bit for ZooKeeper to detect the session closure
        Thread.sleep(2000);

        // Create a new client, simulating a different node taking over
        System.out.println("New node taking over...");
        DistributedQueue recoveryQueue = new DistributedQueue(zkServers, queueName);

        // Check if items are still in the queue
        int size = recoveryQueue.size();
        System.out.println("Queue size after node failure: " + size);

        // Try to dequeue items
        System.out.println("Trying to dequeue items after node failure...");
        for (int i = 0; i < 5; i++) {
            String item = recoveryQueue.dequeueString();
            if (item != null) {
                System.out.println("Successfully dequeued: " + item);
            } else {
                System.out.println("No more items in queue");
                break;
            }
        }

        recoveryQueue.close();
        System.out.println("Node failure test completed");
    }

    /**
     * Views the current queue status.
     *
     * @param zkServers ZooKeeper servers
     * @param queueName Queue name
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void viewQueueStatus(String zkServers, String queueName)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("\n=== Queue Status ===");

        DistributedQueue queue = new DistributedQueue(zkServers, queueName);
        int size = queue.size();

        System.out.println("Queue: " + queueName);
        System.out.println("Size: " + size);

        if (size > 0) {
            byte[] peekedData = queue.peek();
            if (peekedData != null) {
                String peekedItem = new String(peekedData);
                System.out.println("First item: " + peekedItem);
            }
        }

        queue.close();
    }


}