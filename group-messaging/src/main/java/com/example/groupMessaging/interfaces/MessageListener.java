package com.example.groupMessaging;

public interface MessageListener {
    /**
     * Called when a new message is received.
     * @param groupId The ID of the group
     * @param message The received message
     * @param senderId The ID of the sender
     */
    void onMessageReceived(String groupId, String message, String senderId);
}
