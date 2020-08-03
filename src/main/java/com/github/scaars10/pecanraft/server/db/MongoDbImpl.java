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

import static com.mongodb.client.model.Filters.eq;

public class MongoDbImpl implements DbBase
{
    private ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private MongoCollection<Document> commLogCollection, uncommLogCollection, fieldCollection;

    public MongoDbImpl(long id)
    {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("pecanDb");
        commLogCollection = database.getCollection("node_committedLog_"+id);
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
    public void persistFieldToDb(long currentTerm, int votedFor)
    {
        dbLock.writeLock().lock();
        Document doc = new Document("id",1).append("term", currentTerm)
                .append("votedFor", votedFor);

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
    public Pair<Long, Integer> getFields()
    {
        dbLock.readLock().lock();
        if(fieldCollection.countDocuments()==0)
            return null;
        Document doc = fieldCollection.find().first();
        dbLock.readLock().unlock();
        assert doc != null;
        return new Pair<>(doc.getLong("term"), doc.getInteger("votedFor"));
    }
}