package com.github.scaars10.pecan.RMI;

import com.github.scaars10.pecan.structures.LogEntry;

import java.rmi.Remote;

public interface RmiInterface extends Remote {
    public int RequestVote(long term, int candidateId, long lastIndex, long lastTerm) throws Exception;
    public int AppendEntries(long term, int leaderId, long prevLogIndex,
                             long prevLogTerm, LogEntry[] entries, long commitIndex) throws Exception;
}
