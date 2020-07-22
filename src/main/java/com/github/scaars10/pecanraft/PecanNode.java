package com.github.scaars10.pecanraft;

import com.github.scaars10.pecanraft.structures.LogEntry;


import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * The type Pecan node.
 */
public class PecanNode {

    ReentrantReadWriteLock nodeLock = new ReentrantReadWriteLock();
    //interval after which leader sends a heartbeat
    int heartbeat = 50;

    //interval after which follower is allowed to become a candidate if a heartbeat is not received from leader
    int leaderTimeout = 1000;
    /**
     * All possible States
     */
    public enum possibleStates  {
        /**
         * Follower possible states.
         */
        FOLLOWER,
        /**
         * Candidate possible states.
         */
        CANDIDATE,
        /**
         * Leader possible states.
         */
        LEADER
    }

    /**
     * Node identifier
     */
    int id;
    /**
     * Id of known leader according to this node
     */
    int leaderId;
    AtomicInteger votedFor = new AtomicInteger();
    /**
     * Current term according to this node
     */
    long currentTerm = 0;
    ArrayList<LogEntry> log = new ArrayList<LogEntry>();
    int commitIndex = -1;
    int lastApplied = -1;
    /**
     * Stores the ids of peers
     */
    int []peerId;

    /**
     * Initial Node state. All nodes are followers in the beginning.
     */
    possibleStates nodeState = possibleStates.FOLLOWER;

    /**
     * Instantiates a new Pecan node.
     *
     * @param id     the id
     * @param peerId list of peer id(all nodes including itself)
     */

    PecanNode(int id, int []peerId){
        this.id = id;
        this.peerId = peerId;
    }




}
