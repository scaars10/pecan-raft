package com.github.scaars10.pecan;


import com.github.scaars10.pecan.RMI.RmiInterface;
import com.github.scaars10.pecan.structures.LogEntry;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PecanServer extends UnicastRemoteObject implements RmiInterface, Runnable {
    PecanNode node;

    public PecanServer(int id, int[]peerId) throws RemoteException, MalformedURLException {
        super();

        this.node = new PecanNode(id, peerId);

        try {
            Naming.rebind("Node-"+node.id, this);
        } catch (RemoteException e) {
            System.out.println("Remote exception while binding");
            e.printStackTrace();
            throw e;
        } catch (MalformedURLException e) {
            System.out.println("MalformedURL Exception exception while binding");
            e.printStackTrace();
            throw e;
        }

        System.out.println("Object for node"+id+" created");
    }

//    ScheduledExecutorService electionTimer =
//            Executors.newScheduledThreadPool(1);
    final ExecutorService electionExecutor = Executors.newFixedThreadPool(1);
    Future<?> electionFuture;

    /**
     * Class for a thread to run in parallel when timer on the node times out and election starts.
     */
    void startLeader()
    {

    }
    void startFollower()
    {
        
        startElectionTimer();
    }
    void startElection()
    {
        //Startup Election
        System.out.println("Timed out. No heartbeat received from leader with (id-"+node.leaderId+"). Starting a new election");

        //obtain a writelock to ensure safety
        node.nodeLock.readLock().lock();
        //change the state to candidate
        node.nodeState = PecanNode.possibleStates.CANDIDATE;
        node.votedFor.set(node.id);
        node.currentTerm++;
        AtomicInteger voteCount = new AtomicInteger(1);int lastLogTerm = 0;
        if(node.log.size()>0){
            lastLogTerm = node.log.get(node.log.size()-1).getTerm();
        }
        //release the writelock
        node.nodeLock.readLock().lock();

        final ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);

        //create a list of Futures, so you know when each tick is done
        final List<Future> futures = new ArrayList<>();
        for(int i=0;i<node.peerId.length;i++)
        {
            if(node.peerId[i]==node.id)
                continue;

            int finalI = i;
            int finalLastLogTerm = lastLogTerm;
            final Future<?> future = exec.submit(() -> {

                try {
                    //Request vote from peer [i]
                    RmiInterface peer = (RmiInterface) Naming.lookup("Node-" + node.peerId[finalI]);
                    int response = peer.RequestVote(node.currentTerm, node.id, node.commitIndex, finalLastLogTerm);
                    if (response == 0) {
                        voteCount.incrementAndGet();
                    }
                }
                //catching Exceptions
                catch (NotBoundException e) {
                    System.out.println("Not Bound Exception for - node-" + node.peerId[finalI]);
                    e.printStackTrace();
                } catch (MalformedURLException e) {
                    System.out.println("Malformed URL Exception for - node-" + node.peerId[finalI]);
                    e.printStackTrace();
                } catch (RemoteException e) {
                    System.out.println("Remote Exception for - node-" + node.peerId[finalI]);
                    e.printStackTrace();
                } catch (Exception e) {
                    System.out.println("General Exception for - node-" + node.peerId[finalI]);
                    e.printStackTrace();
                }
            });
            futures.add(future);

        }
        for(int i=0;i<100;i++)
        {
            try
            {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (final Future<?> future : futures)
            {
                try {
                    if(future.isDone())
                        future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //stop waiting if a new leader was elected meanwhile or this candidate has obtained majority of votes
            if(node.nodeState== PecanNode.possibleStates.FOLLOWER || voteCount.get()>(node.peerId.length-1)/2)
            {
                for (final Future<?> future : futures)
                {
                    try {
                        if(!future.isDone())
                            future.cancel(true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            }

        }
        if(voteCount.get()>(node.peerId.length-1)/2)
        {
            node.nodeState = PecanNode.possibleStates.LEADER;
            startLeader();
        }

    }




    //returns true if the server making the request is behind
    boolean checkIfServerIsBehind(long term, long lastIndex, long lastLogTerm)
    {
        return node.currentTerm > term || ((node.lastApplied > lastIndex) &&
                (node.log.get(node.log.size()-1).getTerm() >= lastLogTerm));
    }
    @Override
    public int AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm,
                             LogEntry[] entries, long commitIndex)
    {
        node.nodeLock.readLock().lock();
        if(checkIfServerIsBehind(term, prevLogIndex, prevLogTerm))
        {
            node.nodeLock.readLock().unlock();
            return -1;
        }
        return 0;
    }

    public int RequestVote(long term, int candidateId, long prevLogIndex, long prevLogTerm)
    {
        node.nodeLock.writeLock().lock();
        int res = -1;
        if(node.votedFor.get()==0 || node.votedFor.get()==candidateId || !checkIfServerIsBehind(term, prevLogIndex, prevLogTerm))
        {
            node.votedFor.set(candidateId);
            res = 0;
        }
        node.nodeLock.writeLock().unlock();
        if(res==0)
        {
            stopElectionTimer();
            startElectionTimer();
        }
        return res;
    }

    void  startElectionTimer()
    {
        Random rand= new Random();
        int randomTime = (int) (node.leaderTimeout + rand.nextDouble()*150);
        electionFuture = electionExecutor.submit(() -> {
            try {
                Thread.sleep(randomTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            startElection();
        });
//        new Thread(() -> {
//            ScheduledFuture election = electionTimer.schedule(new StartElection(),
//                    randomTime, TimeUnit.MILLISECONDS);
//        }).start();





    }

    void stopElectionTimer()
    {
        electionFuture.cancel(true);
    }
    public void run()
    {
        startFollower();

    }
//    public static void main(String[] args)  {
//        int peerId[] = new int[1];
//        try {
//            PecanServer server = new PecanServer(0, peerId);
//        } catch (Exception e)
//        {
//            System.out.println("Error while creating object");
//            e.printStackTrace();
//        }
//    }
}
