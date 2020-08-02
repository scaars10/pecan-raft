package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;
import javafx.util.Pair;

import java.util.List;

public interface dbBase
{
    void persistLogToDb(List<LogEntry> logs);
    void persistFieldToDb(long currentTerm, int votedFor);
    List<LogEntry> readLogFromDb();
    Pair<Long, Long> getFields();

}
