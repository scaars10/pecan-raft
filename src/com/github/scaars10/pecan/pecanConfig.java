package com.github.scaars10.pecan;

public class pecanConfig {
    //interval after which leader sends a heartbeat
    int heartbeat = 50;

    //interval after which follower is allowed to become a candidate if a heartbeat is not received from leader
    int leaderTimeout = 2000;


}
