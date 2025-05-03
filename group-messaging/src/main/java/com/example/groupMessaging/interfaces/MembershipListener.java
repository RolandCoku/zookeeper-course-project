package com.example.groupMessaging.interfaces;

public interface MembershipListener {
    /**
     * Called when a new member joins the group.
     * @param groupId The ID of the group that the member joined.
     * @param memberId The ID of the member that joined.
     */
    void onMemberJoined(String groupId, String memberId);

    /**
     * Called when a member leaves the group.
     *
     * @param groupId The ID of the group that the member left.
     * @param memberId The ID of the member that left.
     */
    void onMemberLeft(String groupId, String memberId);
}
