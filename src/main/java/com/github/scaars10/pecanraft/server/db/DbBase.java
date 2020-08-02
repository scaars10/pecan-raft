package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;
import javafx.util.Pair;

import java.util.List;

public interface DbBase
{
    void writeCommittedLogs(List<LogEntry> logs);
    void writeUncommittedLogs(List<LogEntry> logs);
    void persistFieldToDb(long currentTerm, int votedFor);
    List<LogEntry> readCommLogsFromDb();
    List<LogEntry> readUnCommLogsFromDb();
    Pair<Long, Integer> getFields();

}
