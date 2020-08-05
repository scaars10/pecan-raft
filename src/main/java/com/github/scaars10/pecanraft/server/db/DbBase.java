package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;
import javafx.util.Pair;

import java.util.List;
import java.util.Map;

public interface DbBase
{
    void writeCommittedLogs(List<LogEntry> logs);
    void writeLogs(List<LogEntry> logs);
    List<LogEntry> readLogs();
    void deleteLogs(long startIndex, long endIndex);
    void writeUncommittedLogs(List<LogEntry> logs);
    void persistFieldToDb(long currentTerm, int votedFor, long commitIndex);
    void updateFields(long currentTerm, int votedFor, long commitIndex);
    List<LogEntry> readCommLogsFromDb();
    List<LogEntry> readUnCommLogsFromDb();
    Map<String, Long> getFields();

}
