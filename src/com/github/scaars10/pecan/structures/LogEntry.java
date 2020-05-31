package com.github.scaars10.pecan.structures;

public class LogEntry {
    int term,key,value;
    LogEntry(int term, int key, int value){
        this.term = term;
        this.key = key;
        this.value = value;
    }

    public int getTerm() {
        return term;
    }
}
