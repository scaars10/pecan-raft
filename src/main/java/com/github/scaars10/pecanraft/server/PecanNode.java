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
    int leaderTimeout = 1000;

    public AtomicInteger getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(AtomicInteger votedFor) {
        this.votedFor = votedFor;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
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
    int leaderId;
    AtomicInteger votedFor = new AtomicInteger();
    /**
     * Current term according to this node
     */
    private long currentTerm = 0;
    private ArrayList<LogEntry> committedLog = new ArrayList<LogEntry>();
    private ArrayList<LogEntry> uncommittedLog = new ArrayList<LogEntry>();
    private long commitIndex = -1;
    int lastApplied = -1; //index of the highest log entry applied to State Machine
    /**
     * peerId Stores the ids of peers and nextIndex stores the index of the next entry to be sent
     * to those nodes as a leader
     */
    int []peerId, nextIndex;

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
    public PecanNode(int id, int []peerId)
    {

        this.id = id;
        this.peerId = peerId;
        nextIndex = new int[peerId.length];
        Arrays.fill(nextIndex, -1);
        loadLogs();
        loadFields();
        db = new MongoDbImpl(id);
        logMessage("Node created");

    }

    public LogEntry getLog(long searchIndex)
    {
        if(searchIndex<=commitIndex)
        {
            return committedLog.get((int)searchIndex);
        }
        if(searchIndex>commitIndex+uncommittedLog.size())
        {
            return null;
        }
        return uncommittedLog.get((int)(searchIndex - commitIndex-1)
        );
    }
    public void persistToDb(long commitIndex)
    {

    }

    public void logMessage(String message)
    {
        logger.info("Info for Node-{} :- {}",id, message);
    }

    public void logError(String message)
    {
        logger.error("Error for Node-{} :- {}",id, message);
    }

    public void updateUncommittedLog(List<RpcLogEntry> list, long nodeMatchIndex, long leaderMatchIndex)
    {

    }

    public void loadLogs()
    {
        committedLog = (ArrayList<LogEntry>) db.readCommLogsFromDb();
        if(committedLog==null)
            committedLog = new ArrayList<>();

        uncommittedLog = (ArrayList<LogEntry>) db.readUnCommLogsFromDb();
        if(uncommittedLog==null)
            uncommittedLog = new ArrayList<>();
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
    }
    public LogEntry getLastLog()
    {
        if(uncommittedLog.size()>0)
        {
            return uncommittedLog.get(uncommittedLog.size()-1);
        }
        else
        {
            return committedLog.get(committedLog.size()-1);
        }
    }

    public List<LogEntry> getLogs(int start, int end)
    {

        if(end == -1)
        {
            end = committedLog.size() + uncommittedLog.size()-1;
        }
        List <LogEntry> result = new ArrayList<>(end-start+1);
        while(start<committedLog.size())
        {
            result.add(committedLog.get(start));
            start++;
        }
        while(start<=end)
        {
            result.add(uncommittedLog.get(start-committedLog.size()));
            start++;
        }
        return result;
    }
    public LogEntry getLastCommittedLog()
    {
        return committedLog.get(committedLog.size()-1);
    }


}
