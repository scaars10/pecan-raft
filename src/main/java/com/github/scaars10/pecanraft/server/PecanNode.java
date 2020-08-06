package com.github.scaars10.pecanraft.server;

import com.github.scaars10.pecanraft.RpcLogEntry;
import com.github.scaars10.pecanraft.server.db.DbBase;
import com.github.scaars10.pecanraft.server.db.MongoDbImpl;
import com.github.scaars10.pecanraft.structures.LogEntry;

import javafx.util.Pair;



import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The type Pecan node.
 */
public class PecanNode {

    private static final Logger logger = LogManager.getLogger(PecanNode.class);
    DbBase db;
    ReentrantReadWriteLock nodeLock = new ReentrantReadWriteLock();
    ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
    //interval after which leader sends a heartbeat
    int heartbeat = 50;

    //interval after which follower is allowed to become a candidate if a heartbeat is not received from leader
    int leaderTimeout = 10000;

    public int getVotedFor() {
        return votedFor.get();
    }

    public void setVotedFor(int newVotedFor) {

        this.votedFor.set(newVotedFor);
        db.updateFields(currentTerm, newVotedFor, commitIndex);
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
        db.updateFields(currentTerm, votedFor.get(), commitIndex);
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
        db.updateFields(currentTerm, votedFor.get(), commitIndex);
    }

    /**
     * All possible States
     */
    public enum possibleStates  {

        FOLLOWER,

        CANDIDATE,

        LEADER
    }

    /**
     * Node identifier
     */
    int id;
    /**
     * Id of known leader according to this node
     */
    int leaderId = -1;
    private AtomicInteger votedFor = new AtomicInteger(-1);
    /**
     * Current term according to this node
     */
    private long currentTerm = 0;
    private List<LogEntry> committedLog = new ArrayList<>();
    private ArrayList<LogEntry> uncommittedLog = new ArrayList<>();
    private List<LogEntry> logs;
    private long commitIndex = -1;
    int lastApplied = -1; //index of the highest log entry applied to State Machine
    /**
     * peerId Stores the ids of peers and nextIndex stores the index of the next entry to be sent
     * to those nodes as a leader
     */
    int []peerId;long[] nextIndex;

    /**
     * Initial Node state. All nodes are followers in the beginning.
     */
    possibleStates nodeState = possibleStates.FOLLOWER;

    //Method to add new entries to uncommitted log
    public void addToUncommittedLog(int key, int value)
    {
        LogEntry lastLog = getLastLog();
        long lastIndex;
        if(lastLog == null)
            lastIndex = -1;
        else
            lastIndex = lastLog.getIndex();
        uncommittedLog.add(new LogEntry(currentTerm, key, value, lastIndex+1));

    }
    public PecanNode(int id, PecanConfig config)
    {

        this.id = id;
        this.peerId = config.getPeerId();
        nextIndex = new long[peerId.length];
        Arrays.fill(nextIndex, -1);
        db = new MongoDbImpl(id);
        //Read logs and state from database
        loadLogs();
        loadFields();

        writeMessage("Node created");

    }

    public LogEntry getLog(long searchIndex)
    {
        if(searchIndex>=logs.size() || searchIndex<0)
            return null;
        return logs.get((int)searchIndex);
    }
    public void persistToDb(long commitIndex)
    {

    }

    public void writeMessage(String message)
    {
        logger.info("Info for Node-{} :- {}",id, message);
    }

    public void writeDebugMsg(String message)
    {

        logger.debug("Info for Node-{} :- {}",id, message);
    }
    public void logError(String message)
    {
        logger.error("Error for Node-{} :- {}",id, message);
    }

    public LogEntry rpcLogToLog(RpcLogEntry log)
    {
        return new LogEntry(log.getTerm(), (int)log.getKey(), (int)log.getValue(), log.getIndex());
    }
    public List<LogEntry> rpcLogsToLogs(List<RpcLogEntry> rpcLogs)
    {
        List<LogEntry> res = new ArrayList<>();
        rpcLogs.forEach(rpcLog->res.add(rpcLogToLog(rpcLog)));
        return res;
    }

    public void updateUncommittedLog(List<RpcLogEntry> list, long nodeMatchIndex, long leaderMatchIndex)
    {
        logs.subList((int)nodeMatchIndex, logs.size()).clear();
        List<LogEntry> rpcLogs =  rpcLogsToLogs(list);
        logs.addAll(rpcLogs);
        db.deleteLogs(nodeMatchIndex, logs.size());
        db.writeLogs(rpcLogs);
    }

    public void loadLogs()
    {

        logs = db.readLogs();
        if(logs == null)
        {
            logs = new ArrayList<>();
        }
    }

    public long getLogSize()
    {
        return logs.size();
    }
    public void loadFields()
    {
        Map<String, Long> map = db.getFields();
        if(map!=null)
        {
            currentTerm = map.get("term");
            //workaround to convert Long to int
            long temp = map.get("votedFor");
            votedFor.set((int) temp);
            commitIndex = map.get("commitIndex");
        }
        else
        {
            db.persistFieldToDb(currentTerm, votedFor.get(), commitIndex);
        }
    }
    public LogEntry getLastLog()
    {

        if(logs.size()>0)
        {
            return logs.get(logs.size()-1);
        }
        return null;
    }

    public List<LogEntry> getLogs(int start, int end)
    {

        if(end == -1)
        {
            end = logs.size()-1;
        }
        if(end>logs.size() || logs.size()==0)
            return null;
        List <LogEntry> result = new ArrayList<>(end-start+1);
        while(start<=end)
        {
            result.add(logs.get(start));
            start++;
        }
        return logs;

    }
    public LogEntry getLastCommittedLog()
    {
        //return committedLog.get(committedLog.size()-1);
        if(commitIndex>=0)
        {
            return logs.get((int)commitIndex);
        }
        return null;
    }


}
