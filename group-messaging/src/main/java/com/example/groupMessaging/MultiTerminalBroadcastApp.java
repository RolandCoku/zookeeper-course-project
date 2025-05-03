package com.example.groupMessaging;

import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import com.example.groupMessaging.utils.ClientHandler;

/**
 * Multi-terminal version of the broadcast application with improved message display
 */
public class MultiTerminalBroadcastApp {
    private static final String DEFAULT_ZK_SERVERS = "localhost:2181";
    private static final int SERVER_PORT = 9876;
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_YELLOW = "\u001B[33m";

    // Flag to indicate if a menu is currently being displayed
    private static final AtomicBoolean displayingMenu = new AtomicBoolean(false);

    /**
     * Main entry point
     *
     * @param args Command-line arguments:
     *             [ZooKeeper servers] [Mode: "server" or "member"] [Member ID]
     */
    public static void main(String[] args) {
        try {
            // Parse arguments
            String zkServers = args.length > 0 ? args[0] : DEFAULT_ZK_SERVERS;
            String mode = args.length > 1 ? args[1] : "server";
            String memberId = args.length > 2 ? args[2] : "member-" + UUID.randomUUID().toString().substring(0, 8);

            System.out.println("Starting Multi-Terminal Broadcast App");
            System.out.println("ZooKeeper servers: " + zkServers);
            System.out.println("Mode: " + mode);

            if (mode.equals("server")) {
                runServerMode(zkServers);
            } else if (mode.equals("member")) {
                System.out.println("Member ID: " + memberId);
                runMemberMode(zkServers, memberId);
            } else {
                System.err.println("Invalid mode: " + mode);
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Runs the server mode
     *
     * @param zkServers ZooKeeper servers
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If operation is interrupted
     */
    private static void runServerMode(String zkServers) throws IOException, InterruptedException {
        System.out.println("=== Running in SERVER mode ===");

        // Start server socket
        try (ServerSocket serverSocket = new ServerSocket(SERVER_PORT)) {
            System.out.println("Server started, listening on port " + SERVER_PORT);

            // Launch a menu in a separate thread
            Thread menuThread = new Thread(() -> {
                try {
                    showServerMenu(zkServers);
                } catch (IOException e) {
                    System.err.println("Error in menu thread: " + e.getMessage());
                }
            });
            menuThread.setDaemon(true);
            menuThread.start();

            // Accept client connections
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                // Start client handler
                Thread handlerThread = new Thread(new ClientHandler(clientSocket));
                handlerThread.setDaemon(true);
                handlerThread.start();
            }
        }
    }

    /**
     * Shows the server menu
     *
     * @param zkServers ZooKeeper servers
     * @throws IOException If I/O error occurs
     */
    private static void showServerMenu(String zkServers) throws IOException {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n=== Multi-Terminal Broadcast Server Menu ===");
            System.out.println("1. Start new member terminal");
            System.out.println("2. Exit");
            System.out.print("Enter choice: ");

            int choice = scanner.nextInt();
            scanner.nextLine();  // Consume newline

            switch (choice) {
                case 1:
                    startMemberInNewTerminal(zkServers);
                    break;
                case 2:
                    System.out.println("Exiting server...");
                    System.exit(0);
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    /**
     * Starts a new member in a separate terminal
     *
     * @param zkServers ZooKeeper servers
     * @throws IOException If I/O error occurs
     */
    private static void startMemberInNewTerminal(String zkServers) throws IOException {
        System.out.println("\n=== Starting New Member Terminal ===");

        String memberId = "member-" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("Member ID: " + memberId);

        String javaCmd = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = MultiTerminalBroadcastApp.class.getName();

        // Prepare command based on OS
        ProcessBuilder processBuilder = new ProcessBuilder();
        String os = System.getProperty("os.name").toLowerCase();

        if (os.contains("win")) {
            // Windows
            processBuilder.command("cmd", "/c", "start", "cmd", "/k",
                    javaCmd + " -cp " + classpath + " " + className + " " +
                            zkServers + " member " + memberId);
        } else if (os.contains("nix") || os.contains("nux") || os.contains("mac")) {
            // Linux or Mac
            processBuilder.command("xterm", "-e",
                    javaCmd + " -cp " + classpath + " " + className + " " +
                            zkServers + " member " + memberId);
        } else {
            System.err.println("Unsupported operating system: " + os);
            return;
        }

        // Start the process
        Process process = processBuilder.start();
        System.out.println("Member started in new terminal.");
    }

    /**
     * Runs the member mode
     *
     * @param zkServers ZooKeeper servers
     * @param memberId Member ID
     * @throws IOException If I/O error occurs
     * @throws InterruptedException If operation is interrupted
     * @throws KeeperException If ZooKeeper operation fails
     */
    private static void runMemberMode(String zkServers, String memberId)
            throws IOException, InterruptedException, KeeperException {
        System.out.println("=== Running in MEMBER mode ===");

        // Connect to server
        Socket socket = new Socket("localhost", SERVER_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Register with server
        out.println("REGISTER|MEMBER|" + memberId);

        // Create message listener with improved display
        MessageListener messageListener = (groupId, message, senderId) -> {
            // Clear the current line if a menu is being displayed
            if (displayingMenu.get()) {
                System.out.print("\r                                                                              \r");
            }

            // Print the message with color and formatting
            System.out.println();
            System.out.println(ANSI_GREEN + "┌─── New Message ───┐" + ANSI_RESET);
            System.out.println(ANSI_BLUE + "│ Group: " + ANSI_RESET + groupId);
            System.out.println(ANSI_BLUE + "│ From:  " + ANSI_RESET + senderId);
            System.out.println(ANSI_BLUE + "│ " + ANSI_RESET + message);
            System.out.println(ANSI_GREEN + "└───────────────────┘" + ANSI_RESET);

            // Re-display menu prompt if it was showing
            if (displayingMenu.get()) {
                System.out.print("Enter choice: ");
            }
        };

        // Create membership listener with improved display
        GroupMember member = getGroupMember(zkServers, memberId, messageListener);

        // Start command listener thread
        Thread commandThread = new Thread(() -> {
            try {
                String command;
                while ((command = in.readLine()) != null) {
                    processCommand(command, member);
                }
            } catch (IOException | KeeperException | InterruptedException e) {
                System.err.println("Error in command thread: " + e.getMessage());
            }
        });
        commandThread.setDaemon(true);
        commandThread.start();

        // Show member menu
        showMemberMenu(member, out);

        // Cleanup
        member.close();
        socket.close();
    }

    private static GroupMember getGroupMember(String zkServers, String memberId, MessageListener messageListener) throws IOException, InterruptedException, KeeperException {
        MembershipListener membershipListener = new MembershipListener() {
            @Override
            public void onMemberJoined(String groupId, String joinedMemberId) {
                // Clear the current line if a menu is being displayed
                if (displayingMenu.get()) {
                    System.out.print("\r                                                                              \r");
                }

                // Print the notification with color
                System.out.println();
                System.out.println(ANSI_YELLOW + "► Member joined: " + joinedMemberId + " in group " + groupId + ANSI_RESET);

                // Re-display menu prompt if it was showing
                if (displayingMenu.get()) {
                    System.out.print("Enter choice: ");
                }
            }

            @Override
            public void onMemberLeft(String groupId, String leftMemberId) {
                // Clear the current line if a menu is being displayed
                if (displayingMenu.get()) {
                    System.out.print("\r                                                                              \r");
                }

                // Print the notification with color
                System.out.println();
                System.out.println(ANSI_YELLOW + "◄ Member left: " + leftMemberId + " from group " + groupId + ANSI_RESET);

                // Re-display menu prompt if it was showing
                if (displayingMenu.get()) {
                    System.out.print("Enter choice: ");
                }
            }
        };

        // Create group member
        return new GroupMember(zkServers, memberId, messageListener, membershipListener);
    }

    /**
     * Shows the member menu
     *
     * @param member Group member
     * @param out Output writer for sending commands to server
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private static void showMemberMenu(GroupMember member, PrintWriter out)
            throws KeeperException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        List<String> joinedGroups = new ArrayList<>();

        while (true) {
            System.out.println("\n=== Member Menu ===");
            System.out.println("1. Join group");
            System.out.println("2. Leave group");
            System.out.println("3. Send message");
            System.out.println("4. List joined groups");
            System.out.println("5. Exit");

            // Set flag to indicate a menu is being displayed
            displayingMenu.set(true);
            System.out.print("Enter choice: ");

            int choice = scanner.nextInt();
            scanner.nextLine();  // Consume newline

            // Clear flag after input is received
            displayingMenu.set(false);

            switch (choice) {
                case 1:
                    System.out.print("Enter group ID: ");
                    String groupId = scanner.nextLine();
                    member.joinGroup(groupId);
                    joinedGroups.add(groupId);
                    out.println("JOINED|" + groupId);
                    System.out.println(ANSI_GREEN + "Successfully joined group: " + groupId + ANSI_RESET);
                    break;

                case 2:
                    if (joinedGroups.isEmpty()) {
                        System.out.println("You haven't joined any groups yet.");
                        break;
                    }

                    System.out.println("Select group to leave:");
                    for (int i = 0; i < joinedGroups.size(); i++) {
                        System.out.println((i + 1) + ". " + joinedGroups.get(i));
                    }
                    System.out.print("Enter number: ");

                    int groupIndex = scanner.nextInt();
                    scanner.nextLine();  // Consume newline

                    if (groupIndex > 0 && groupIndex <= joinedGroups.size()) {
                        groupId = joinedGroups.get(groupIndex - 1);
                        member.leaveGroup(groupId);
                        joinedGroups.remove(groupId);
                        out.println("LEFT|" + groupId);
                        System.out.println(ANSI_YELLOW + "Left group: " + groupId + ANSI_RESET);
                    } else {
                        System.out.println("Invalid group number.");
                    }
                    break;

                case 3:
                    if (joinedGroups.isEmpty()) {
                        System.out.println("You haven't joined any groups yet.");
                        break;
                    }

                    System.out.println("Select group to send message to:");
                    for (int i = 0; i < joinedGroups.size(); i++) {
                        System.out.println((i + 1) + ". " + joinedGroups.get(i));
                    }
                    System.out.print("Enter number: ");

                    groupIndex = scanner.nextInt();
                    scanner.nextLine();  // Consume newline

                    if (groupIndex > 0 && groupIndex <= joinedGroups.size()) {
                        groupId = joinedGroups.get(groupIndex - 1);

                        System.out.print("Enter message: ");
                        String message = scanner.nextLine();

                        member.broadcast(groupId, message);
                        out.println("BROADCAST|" + groupId + "|" + message);

                        // Display confirmation that message was sent
                        System.out.println(ANSI_GREEN + "Message sent to group: " + groupId + ANSI_RESET);
                    } else {
                        System.out.println("Invalid group number.");
                    }
                    break;

                case 4:
                    System.out.println("\n=== Joined Groups ===");
                    if (joinedGroups.isEmpty()) {
                        System.out.println("You haven't joined any groups yet.");
                    } else {
                        for (int i = 0; i < joinedGroups.size(); i++) {
                            System.out.println((i + 1) + ". " + joinedGroups.get(i));
                        }
                    }
                    break;

                case 5:
                    System.out.println("Exiting...");
                    // Leave all groups before exiting
                    for (String group : joinedGroups) {
                        member.leaveGroup(group);
                        out.println("LEFT|" + group);
                    }
                    out.println("EXIT");
                    return;

                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
    }

    /**
     * Processes a command received from the server
     *
     * @param command Command to process
     * @param member Group member
     * @throws KeeperException If ZooKeeper operation fails
     * @throws InterruptedException If operation is interrupted
     */
    private static void processCommand(String command, GroupMember member)
            throws KeeperException, InterruptedException {
        String[] parts = command.split("\\|");

        if (parts.length > 0) {
            String action = parts[0];

            switch (action) {
                case "JOIN":
                    if (parts.length > 1) {
                        String groupId = parts[1];
                        member.joinGroup(groupId);
                    }
                    break;

                case "LEAVE":
                    if (parts.length > 1) {
                        String groupId = parts[1];
                        member.leaveGroup(groupId);
                    }
                    break;

                case "BROADCAST":
                    if (parts.length > 2) {
                        String groupId = parts[1];
                        String message = parts[2];
                        member.broadcast(groupId, message);
                    }
                    break;

                case "EXIT":
                    System.out.println("Received exit command from server");
                    System.exit(0);
                    break;
            }
        }
    }
}