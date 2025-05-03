package com.example.queue.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Client handler for communication with clients.
 */
public class ClientHandler implements Runnable {
    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;
    private final String clientId;
    private String clientType = "UNKNOWN";

    /**
     * Creates a new client handler.
     *
     * @param socket Client socket
     * @throws IOException If I/O error occurs
     */
    public ClientHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.clientId = "client-" + socket.getInetAddress() + ":" + socket.getPort();
    }

    @Override
    public void run() {
        try {
            String message;
            while ((message = in.readLine()) != null) {
                if (message.startsWith("REGISTER")) {
                    String[] parts = message.split("\\|");
                    if (parts.length > 1) {
                        clientType = parts[1];
                        System.out.println("Client " + clientId + " registered as " + clientType);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client " + clientId + ": " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Error closing socket for client " + clientId + ": " + e.getMessage());
            }
        }
    }

    /**
     * Sends a command to the client.
     *
     * @param command Command to send
     */
    public void sendCommand(String command) {
        out.println(command);
    }

    /**
     * Closes the client handler.
     *
     * @throws IOException If I/O error occurs
     */
    public void close() throws IOException {
        socket.close();
    }

    /**
     * Gets the client ID.
     *
     * @return Client ID
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets the client type.
     *
     * @return Client type
     */
    public String getClientType() {
        return clientType;
    }
}