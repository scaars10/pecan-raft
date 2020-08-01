package com.github.scaars10.pecanraft.structures;


public class LogEntry {

    long index;
    int key,value;
    long term;
    public LogEntry(long term, int key, int value, long index){
        this.term = term;
        this.key = key;
        this.value = value;
        this.index  = index;
    }

    public long getTerm() {
        return term;
    }
}
