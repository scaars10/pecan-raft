package com.github.scaars10.pecanraft.Server;

import com.github.scaars10.pecanraft.RpcLogEntry;
import com.github.scaars10.pecanraft.structures.LogEntry;


import java.util.ArrayList;
import java.util.List;
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
    ArrayList<LogEntry> committedLog = new ArrayList<LogEntry>();
    ArrayList<LogEntry> uncommittedLog = new ArrayList<LogEntry>();
    long commitIndex = -1, index = -1;
    int lastApplied = -1;
    /**
     * Stores the ids of peers
     */
    int []peerId;

    /**
     * Initial Node state. All nodes are followers in the beginning.
     */
    possibleStates nodeState = possibleStates.FOLLOWER;

    //Method to add new entries to uncommitted log
    public void addToUncommittedLog(int key, int value)
    {
        index++;
        uncommittedLog.add(new LogEntry(currentTerm, key, value, index));
    }
    public PecanNode(int id, int []peerId){
        this.id = id;
        this.peerId = peerId;
    }

    public LogEntry getLog(long searchIndex)
    {
        if(searchIndex<=commitIndex)
        {
            return committedLog.get((int)searchIndex);
        }
        if(searchIndex>commitIndex+uncommittedLog.size())
        {
            return null;
        }
        return uncommittedLog.get((int)(searchIndex - commitIndex-1)
        );
    }
    public void persistToDb(long commitIndex)
    {

    }

    public void updateUncommittedLog(List<RpcLogEntry> list, long matchIndex)
    {

    }




}
