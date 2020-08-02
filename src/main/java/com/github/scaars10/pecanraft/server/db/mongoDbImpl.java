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

public class mongoDbImpl implements dbBase
{
    private ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();
    private MongoClient mongoClient ;
    private MongoDatabase database;
    private MongoCollection<Document> logCollection, fieldCollection;
    public mongoDbImpl(long id)
    {
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("pecanDb");
        logCollection = database.getCollection("node_log_"+id);
        fieldCollection = database.getCollection("node_field_"+id);
    }

    @Override
    public void persistLogToDb(List<LogEntry> logs)
    {
        dbLock.writeLock().lock();
        logs.parallelStream().forEach((log)->
        {
            Document doc = new Document("index", new ObjectId(String.valueOf(log.getIndex())))
                    .append("term", log.getTerm())
                    .append("value", log.getValue()).append("key", log.getKey());
            logCollection.insertOne(doc);
        });
        dbLock.writeLock().unlock();

    }

    @Override
    public void persistFieldToDb(long currentTerm, int votedFor)
    {
        dbLock.writeLock().lock();
        Document doc = new Document("id",1).append("term", currentTerm)
                .append("votedFor", votedFor);
        Document res = null;
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
    public List<LogEntry> readLogFromDb()
    {
        dbLock.readLock().lock();
        List <LogEntry> list = new ArrayList<>();
        logCollection.find().iterator().forEachRemaining(log-> list.add(documentToLog(log)));
        dbLock.readLock().unlock();
        return list;
    }

    @Override
    public Pair<Long, Long> getFields()
    {
        dbLock.readLock().lock();
        if(fieldCollection.countDocuments()==0)
            return null;
        Document doc = fieldCollection.find().first();
        dbLock.readLock().unlock();
        assert doc != null;
        return new Pair<>(doc.getLong("term"), doc.getLong("votedFor"));
    }
}
