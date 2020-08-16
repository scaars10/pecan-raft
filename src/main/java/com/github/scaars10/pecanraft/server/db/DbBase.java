package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;
import javafx.util.Pair;

import java.util.List;
import java.util.Map;

/**
 * The interface Db base.
 */
public interface DbBase
{
    /**
     * Write committed logs to db.
     *
     * @param logs the logs
     */
    void writeCommittedLogs(List<LogEntry> logs);

    /**
     * Write logs to db.
     *
     * @param logs the logs
     */
    void writeLogs(List<LogEntry> logs);

    /**
     * Read logs from db.
     *
     * @return the list
     */
    List<LogEntry> readLogs();

    /**
     * Delete logs from db.
     *
     * @param startIndex the start index
     * @param endIndex   the end index
     */
    void deleteLogs(long startIndex, long endIndex);

    /**
     * Write uncommitted logs.
     *
     * @param logs the logs
     */
    void writeUncommittedLogs(List<LogEntry> logs);

    /**
     * Persist fields to db.
     *
     * @param currentTerm the current term
     * @param votedFor    the voted for
     * @param commitIndex the commit index
     */
    void persistFieldToDb(long currentTerm, int votedFor, long commitIndex);

    /**
     * Update fields.
     *
     * @param currentTerm the current term
     * @param votedFor    the voted for
     * @param commitIndex the commit index
     */
    void updateFields(long currentTerm, int votedFor, long commitIndex);

    /**
     * Read comm logs from db list.
     *
     * @return the list
     */
    List<LogEntry> readCommLogsFromDb();

    /**
     * Read un comm logs from db list.
     *
     * @return the list
     */
    List<LogEntry> readUnCommLogsFromDb();

    /**
     * Gets fields.
     *
     * @return the fields
     */
    Map<String, Long> getFields();

    /**
     * Add to key value store.
     *
     * @param key   the key
     * @param value the value
     */
    void addToKeyValueStore(int key, int value);

    /**
     * Write log.
     *
     * @param log the log
     */
    void writeLog(LogEntry log);

}
