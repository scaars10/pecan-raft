package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;

import java.util.List;
import java.util.Map;

/**
 * The interface Db base.
 */
public interface DbBase
{

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
