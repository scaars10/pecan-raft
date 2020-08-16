package com.github.scaars10.pecanraft.server;

import com.github.scaars10.pecanraft.RpcLogEntry;
import com.github.scaars10.pecanraft.server.db.DbBase;
import com.github.scaars10.pecanraft.server.db.MongoDbImpl;
import com.github.scaars10.pecanraft.structures.LogEntry;





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
    /**
     * The Db.
     */
    DbBase db;
    /**
     * The Node lock.
     */
    ReentrantReadWriteLock nodeLock = new ReentrantReadWriteLock();
    /**
     * The Log lock.
     */
    ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
    /**
     * Interval after which leader sends a heartbeat
     * The Heartbeat.
     */

    int heartbeat = 150;

    /**
     * Interval after which follower is allowed to become a candidate if a heartbeat is not received from leader
     * The Leader timeout.
     */

    int leaderTimeout = 2000;

    /**
     * Gets voted for.
     *
     * @return the voted for
     */
    public int getVotedFor() {
        return votedFor.get();
    }

    /**
     * Sets voted for.
     *
     * @param newVotedFor the new voted for
     */
    public void setVotedFor(int newVotedFor) {

        this.votedFor.set(newVotedFor);
        db.updateFields(currentTerm, newVotedFor, commitIndex);
    }

    /**
     * Gets current term.
     *
     * @return the current term
     */
    public long getCurrentTerm() {
        return currentTerm;
    }

    /**
     * Sets current term.
     *
     * @param currentTerm the current term
     */
    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
        db.updateFields(currentTerm, votedFor.get(), commitIndex);
    }

    /**
     * Gets commit index.
     *
     * @return the commit index
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Sets commit index and updates key-value store..
     *
     * @param commitIndex the commit index
     */
    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
        db.updateFields(currentTerm, votedFor.get(), commitIndex);
        writeToKeyValue();
        System.out.println(commitIndex);
    }

    /**
     * All possible States
     */
    public enum possibleStates  {

        /**
         * Follower possible states.
         */
        FOLLOWER,

        /**
         * Candidate possible states.
         */
        CANDIDATE,

        /**
         * Leader possible states.
         */
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

    /**
     * List of Logs..
     */
    private List<LogEntry> logs;
    /**
     * Index of log upto which logs have been committed
     */
    private long commitIndex = -1;
    /**
     * The Last applied.
     */
    long lastApplied = -1; //index of the highest log entry applied to State Machine
    /**
     * peerId Stores the ids of peers and nextIndex stores the index of the next entry to be sent
     * to those nodes as a leader
     */
    int []peerId;
    /**
     * The Next index.
     */
    long[] nextIndex;

    /**
     * Initial Node state. All nodes are followers in the beginning.
     */
    possibleStates nodeState = possibleStates.FOLLOWER;

    /**
     * Add to uncommitted log.
     * Method to add new entries to uncommitted log
     * @param key   the key
     * @param value the value
     */

    public void addToUncommittedLog(int key, int value)
    {
        LogEntry lastLog = getLastLog();
        long lastIndex;
        if(lastLog == null)
            lastIndex = -1;
        else
            lastIndex = lastLog.getIndex();
       LogEntry latestLog = new LogEntry(currentTerm, key, value, lastIndex+1);
       logs.add(latestLog);
       db.writeLog(latestLog);

    }

    /**
     * Instantiates a new Pecan node.
     *
     * @param id     the id
     * @param config the config
     */
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

    /**
     * Gets log.
     *
     * @param searchIndex the search index
     * @return the log
     */
    public LogEntry getLog(long searchIndex)
    {
        if(searchIndex>=logs.size() || searchIndex<0)
            return null;
        return logs.get((int)searchIndex);
    }

    /**
     * Write message.
     *
     * @param message the message
     */
/* FIXME:
        Need to work on Logger
     */
    public void writeMessage(String message)
    {
        logger.info("Info for Node-{} :- {}",id, message);
    }

    /**
     * Log error.
     *
     * @param message the message
     */
//
//    public void writeDebugMsg(String message)
//    {
//
//        logger.debug("Info for Node-{} :- {}",id, message);
//    }
    public void logError(String message)
    {
        logger.error("Error for Node-{} :- {}",id, message);
    }

    /**
     * Converts Rpc-log to log.
     *
     * @param log the log
     * @return the log entry
     */
    public LogEntry rpcLogToLog(RpcLogEntry log)
    {
        return new LogEntry(log.getTerm(), (int)log.getKey(), (int)log.getValue(), log.getIndex());
    }

    /**
     * Rpc logs to logs list.
     *
     * @param rpcLogs the rpc logs
     * @return the list
     */
    public List<LogEntry> rpcLogsToLogs(List<RpcLogEntry> rpcLogs)
    {
        List<LogEntry> res = new ArrayList<>();
        rpcLogs.forEach(rpcLog->res.add(rpcLogToLog(rpcLog)));
        return res;
    }

    /**
     * FIXME:
     *  Method can be optimized using Binary Search..
     *
     * Method to update logs
     * @param list           the list
     * @param nodeMatchIndex the node match index
     */
    public void updateUncommittedLog(List<RpcLogEntry> list, long nodeMatchIndex)
    {

        List<LogEntry> rpcLogs =  rpcLogsToLogs(list);

        db.deleteLogs(nodeMatchIndex, logs.size());
        logs.subList((int)nodeMatchIndex, logs.size()).clear();
        logs.addAll(rpcLogs);
        db.writeLogs(rpcLogs);
    }

    /**
     * Load logs.
     */
    public void loadLogs()
    {

        logs = db.readLogs();
        if(logs == null)
        {
            logs = new ArrayList<>();
        }
    }

    /**
     * Gets log size.
     *
     * @return the log size
     */
    public long getLogSize()
    {
        return logs.size();
    }

    /**
     * Load fields.
     */
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
            System.out.println("Loaded Fields - CI "+commitIndex);
        }
        else
        {
            db.persistFieldToDb(currentTerm, votedFor.get(), commitIndex);
        }
        lastApplied = commitIndex;
    }

    /**
     * Gets last log.
     *
     * @return the last log
     */
    public LogEntry getLastLog()
    {

        if(logs.size()>0)
        {
            return logs.get(logs.size()-1);
        }
        return null;
    }

    /**
     * Gets logs in a given range including start and excluding end ..
     *
     * @param start the start
     * @param end   the end
     * @return the logs
     */
    public List<LogEntry> getLogs(int start, int end)
    {

        if(end == -1)
        {
            end = logs.size()-1;
        }
        if(start<=-1)
            start=0;
        if(end>logs.size() || logs.size()==0)
            return null;
        List <LogEntry> result = new ArrayList<>();
        while(start<=end)
        {
            //System.out.println(start);
            result.add(logs.get(start));
            //System.out.println("I "+logs.get(start).getIndex());
            start++;
        }
        //System.out.println("S "+result.size());
        return result;

    }

    /**
     * Gets last committed log.
     *
     * @return the last committed log
     */
    public LogEntry getLastCommittedLog()
    {
        //return committedLog.get(committedLog.size()-1);
        if(commitIndex>=0)
        {
            return logs.get((int)commitIndex);
        }
        return null;
    }


    /**
     * Write to key value.
     */
    public void writeToKeyValue()
    {
        while(lastApplied<commitIndex && commitIndex>=0)
        {
            lastApplied++;
            db.addToKeyValueStore(logs.get((int)lastApplied).getKey(), logs.get((int)lastApplied).getValue());

        }
    }


}
