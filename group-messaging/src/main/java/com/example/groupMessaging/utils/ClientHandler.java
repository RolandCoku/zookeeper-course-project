package com.example.groupMessaging.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Client handler for communication with member clients
 */
public class ClientHandler implements Runnable {
    private final Socket socket;
    private BufferedReader in;
    private String memberId;

    /**
     * Creates a new client handler
     *
     * @param socket Client socket
     */
    public ClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String message;
            while ((message = in.readLine()) != null) {
                String[] parts = message.split("\\|");

                if (parts.length > 0) {
                    String action = parts[0];

                    switch (action) {
                        case "REGISTER":
                            if (parts.length > 2) {
                                memberId = parts[2];
                                System.out.println("Member registered: " + memberId);
                            }
                            break;

                        case "JOINED":
                            if (parts.length > 1) {
                                String groupId = parts[1];
                                System.out.println("Member " + memberId + " joined group: " + groupId);
                            }
                            break;

                        case "LEFT":
                            if (parts.length > 1) {
                                String groupId = parts[1];
                                System.out.println("Member " + memberId + " left group: " + groupId);
                            }
                            break;

                        case "BROADCAST":
                            if (parts.length > 2) {
                                String groupId = parts[1];
                                String msgContent = parts[2];
                                System.out.println("Member " + memberId + " broadcast to " +
                                        groupId + ": " + msgContent);
                            }
                            break;

                        case "EXIT":
                            System.out.println("Member " + memberId + " disconnected");
                            return;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Error closing socket: " + e.getMessage());
            }
        }
    }

}