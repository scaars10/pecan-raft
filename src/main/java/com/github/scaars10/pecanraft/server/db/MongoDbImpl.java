package com.github.scaars10.pecanraft.server.db;

import com.github.scaars10.pecanraft.structures.LogEntry;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;


import java.util.*;


import static com.mongodb.client.model.Filters.*;

/**
 * The type Mongo db.
 */
public class MongoDbImpl implements DbBase
{
    private MongoCollection<Document> logCollection, fieldCollection, keyValueCollection;

    /**
     * Instantiates a new Mongo db.
     *
     * @param id the id
     */
    public MongoDbImpl(long id)
    {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("pecanDb");
        logCollection = database.getCollection("node_"+id+"_logs");
        fieldCollection = database.getCollection("node_field_"+id);
        keyValueCollection = database.getCollection("node_store_"+id);
        System.out.println("Initialized Database connections");
    }



    @Override
    public void writeLog(LogEntry log)
    {
        Document doc = new Document("index", log.getIndex())
                .append("term", log.getTerm())
                .append("value", log.getValue()).append("key", log.getKey());
        logCollection.insertOne(doc);
    }
    @Override
    public void writeLogs(List<LogEntry> logs)
    {
        //dbLock.writeLock().lock();
        logs.forEach(this::writeLog);
        //dbLock.writeLock().unlock();
    }

    @Override
    public List<LogEntry> readLogs()
    {
        //dbLock.readLock().lock();
        List <LogEntry> list = new ArrayList<>();
        if(logCollection.countDocuments()==0)
            return null;
        logCollection.find().iterator().forEachRemaining(log-> list.add(documentToLog(log)));
        list.sort((a, b) ->
                (int) (a.getIndex() - b.getIndex()));
        list.forEach((el)-> System.out.println("El1 -"+el.getIndex()));
        //dbLock.readLock().unlock();
        return list;
    }

    @Override
    public void deleteLogs(long startIndex, long endIndex)
    {
        System.out.println("Initial collection count "+logCollection.countDocuments());
        System.out.println("Deleting logs from "+startIndex+" to "+endIndex);
        logCollection.deleteMany(and(gte("index",startIndex)
                , lt("index", endIndex)));
        System.out.println("collection count "+logCollection.countDocuments());
    }



    @Override
    public void persistFieldToDb(long currentTerm, int votedFor, long commitIndex)
    {
        //dbLock.writeLock().lock();
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
        //dbLock.writeLock().unlock();
    }

    @Override
    public void updateFields(long currentTerm, int votedFor, long commitIndex)
    {
       // System.out.println(currentTerm+" "+votedFor+" "+commitIndex);
        Document temp =fieldCollection.find(eq("id", 1)).first();
       // System.out.println("Doc found..");
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
       // System.out.println("Temp created..");
        Document replacement = new Document("id", 1).append("term", currentTerm)
                .append("votedFor", votedFor).append("commitIndex", commitIndex);
        fieldCollection.replaceOne(eq("id", 1), replacement);
        //System.out.println("Done Updating fields..");
    }

    /**
     * Document to log log entry.
     *
     * @param doc the doc
     * @return the log entry
     */
    public LogEntry documentToLog(Document doc)
    {
        return new LogEntry((long)doc.get("term"),
                (int)doc.get("key"), (int)doc.get("value"), (long) doc.get("index"));
    }





    @Override
    public Map<String, Long> getFields()
    {
        //dbLock.readLock().lock();
        if(fieldCollection.countDocuments()==0)
            return null;
        Document doc = fieldCollection.find().first();
        //dbLock.readLock().unlock();
        assert doc != null;
        Map<String, Long> map = new HashMap<>();
        int temp = doc.getInteger("votedFor");
        map.put("votedFor", (long) temp);
        map.put("term", (long) doc.get("term"));
        map.put("commitIndex", (long) doc.get("commitIndex"));
        return map;
    }

    @Override
    public void addToKeyValueStore(int key, int value)
    {
        Document temp =fieldCollection.find(eq("key", key)).first();
        Document newDoc = new Document("key", key).append("value", value);
        if(temp==null)
        {
            keyValueCollection.insertOne(newDoc);
        }
        else
        {
            keyValueCollection.replaceOne(eq("key", key), newDoc);
        }
    }
}
