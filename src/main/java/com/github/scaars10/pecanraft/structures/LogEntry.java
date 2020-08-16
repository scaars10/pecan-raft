package com.github.scaars10.pecanraft.structures;


/**
 * The type Log entry.
 */
public class LogEntry {

    /**
     * The Index of the log.
     */
    long index;
    /**
     * The Key.
     */
    int key, /**
     * The Value.
     */
    value;
    /**
     * The Term in which the log was received by leader.
     */
    long term;

    /**
     * Instantiates a new Log entry.
     *
     * @param term  the term
     * @param key   the key
     * @param value the value
     * @param index the index
     */
    public LogEntry(long term, int key, int value, long index){
        this.term = term;
        this.key = key;
        this.value = value;
        this.index  = index;
    }

    /**
     * Gets term.
     *
     * @return the term
     */
    public long getTerm() {
        return term;
    }

    /**
     * Gets index.
     *
     * @return the index
     */
    public long getIndex() {
        return index;
    }

    /**
     * Gets key.
     *
     * @return the key
     */
    public int getKey() {
        return key;
    }

    /**
     * Gets value.
     *
     * @return the value
     */
    public int getValue() {
        return value;
    }
}
