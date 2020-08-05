package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import javafx.util.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.mongodb.client.model.Filters.*;

public class MongoDbImpl implements DbBase
{
    private ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private MongoCollection<Document> commLogCollection,
            uncommLogCollection,logCollection, fieldCollection;

    public MongoDbImpl(long id)
    {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("pecanDb");
        commLogCollection = database.getCollection("node_committedLog_"+id);
        logCollection = database.getCollection("node_"+id+"_logs");
        uncommLogCollection = database.getCollection("node_uncommittedLog_"+id);
        fieldCollection = database.getCollection("node_field_"+id);

    }

    @Override
    public void writeCommittedLogs(List<LogEntry> logs)
    {
        dbLock.writeLock().lock();
        logs.parallelStream().forEach((log)->
        {
            Document doc = new Document("index", new ObjectId(String.valueOf(log.getIndex())))
                    .append("term", log.getTerm())
                    .append("value", log.getValue()).append("key", log.getKey());
            commLogCollection.insertOne(doc);
        });
        dbLock.writeLock().unlock();

    }

    @Override
    public void writeLogs(List<LogEntry> logs)
    {
        dbLock.writeLock().lock();
        logs.parallelStream().forEach((log)->
        {
            Document doc = new Document("index", log.getIndex())
                    .append("term", log.getTerm())
                    .append("value", log.getValue()).append("key", log.getKey());
            logCollection.insertOne(doc);
        });
        dbLock.writeLock().unlock();
    }

    @Override
    public List<LogEntry> readLogs()
    {
        dbLock.readLock().lock();
        List <LogEntry> list = new ArrayList<>();
        if(logCollection.countDocuments()==0)
            return null;
        logCollection.find().iterator().forEachRemaining(log-> list.add(documentToLog(log)));
        dbLock.readLock().unlock();
        return list;
    }

    @Override
    public void deleteLogs(long startIndex, long endIndex)
    {
        logCollection.deleteMany(and(gte("index",startIndex)
                , lt("index", endIndex)));
    }

    @Override
    public void writeUncommittedLogs(List<LogEntry> logs)
    {
        dbLock.writeLock().lock();
        logs.parallelStream().forEach((log)->
        {
            Document doc = new Document("index", new ObjectId(String.valueOf(log.getIndex())))
                    .append("term", log.getTerm())
                    .append("value", log.getValue()).append("key", log.getKey());
            uncommLogCollection.insertOne(doc);
        });
        dbLock.writeLock().unlock();
    }

    @Override
    public void persistFieldToDb(long currentTerm, int votedFor, long commitIndex)
    {
        dbLock.writeLock().lock();
        Document doc = new Document("id",1).append("term", currentTerm)
                .append("votedFor", votedFor).append("commitIndex", commitIndex);

        if(fieldCollection.countDocuments()==0)
        {
            fieldCollection.insertOne(doc);
        }
        else
        {
            fieldCollection.updateOne(eq("id", 1), doc);
        }
        dbLock.writeLock().unlock();
    }

    @Override
    public void updateFields(long currentTerm, int votedFor, long commitIndex) {

        Document temp =fieldCollection.find(eq("id", 1)).first();
        if(currentTerm>=0)
        {

            if(temp!=null)
                temp.replace("term", currentTerm);
        }
        if(votedFor>=0)
        {
            if(temp!=null)
                temp.replace("votedFor", votedFor);
        }
        if(commitIndex>=0)
        {
            if(temp!=null)
                temp.replace("commitIndex", commitIndex);
        }
        assert temp != null;
        fieldCollection.updateOne(eq("id", 1), temp);
    }

    public LogEntry documentToLog(Document doc)
    {
        return new LogEntry((long)doc.get("term"),
                (int)doc.get("key"), (int)doc.get("value"), (long) doc.get("index"));
    }

    @Override
    public List<LogEntry> readCommLogsFromDb()
    {
        dbLock.readLock().lock();
        List <LogEntry> list = new ArrayList<>();
        if(commLogCollection.countDocuments()==0)
            return null;
        commLogCollection.find().iterator().forEachRemaining(log-> list.add(documentToLog(log)));
        dbLock.readLock().unlock();
        return list;
    }

    @Override
    public List<LogEntry> readUnCommLogsFromDb()
    {
        dbLock.readLock().lock();
        List <LogEntry> list = new ArrayList<>();
        if(uncommLogCollection.countDocuments()==0)
            return null;
        uncommLogCollection.find().iterator().forEachRemaining(log-> list.add(documentToLog(log)));
        dbLock.readLock().unlock();
        return list;
    }

    @Override
    public Map<String, Long> getFields()
    {
        dbLock.readLock().lock();
        if(fieldCollection.countDocuments()==0)
            return null;
        Document doc = fieldCollection.find().first();
        dbLock.readLock().unlock();
        assert doc != null;
        Map<String, Long> map = new HashMap<>();
        map.put("votedFor", (long) doc.get("votedFor"));
        map.put("term", (long) doc.get("term"));
        map.put("commitIndex", (long) doc.get("commitIndex"));
        return map;
    }
}
