package com.github.scaars10.pecan;


import com.github.scaars10.pecan.RMI.RmiInterface;
import com.github.scaars10.pecan.structures.LogEntry;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class PecanServer extends UnicastRemoteObject implements RmiInterface {
    PecanNode node;

    public PecanServer(int id, int[]peerId) throws Exception{
        this.node = new PecanNode(id, peerId);

        Naming.rebind("Node-"+node.id, this);
    }

    ScheduledExecutorService electionTimer =
            Executors.newScheduledThreadPool(1);


    /**
     * Class for a thread to run in parallel when timer on the node times out and election starts.
     */

    class StartElection implements Runnable{
        public void run(){
            System.out.println("Timed out. No heartbeat received from leader with (id-"+node.leaderId+"). Starting a new election");
            node.nodeState = PecanNode.possibleStates.CANDIDATE;
            node.votedFor = node.id;
            node.currentTerm++;
            int voteCount = 1,lastLogTerm = 0;
            if(node.log.size()>0){
                lastLogTerm = node.log.get(node.log.size()-1).getTerm();
            }
            for(int i=0;i<node.peerId.length;i++)
            {
                if(node.peerId[i]==node.id)
                    continue;
                try
                {
                    RmiInterface peer = (RmiInterface)Naming.lookup("Node-"+node.peerId[i]);
                    int response = peer.RequestVote(node.currentTerm, node.id, node.commitIndex, lastLogTerm);
                    if(response==0)
                    {
                        voteCount++;
                    }
                }
                catch (NotBoundException e)
                {
                    e.printStackTrace();
                }
                catch (MalformedURLException e)
                {
                    e.printStackTrace();
                }
                catch (RemoteException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //Startup Election
        }
    }

    /**
     * Election timer for starting a new election when no heartbeat is received from Leader.
     */

    @Override
    public int AppendEntries(long term, int leaderId,
                             long prevLogIndex, long prevLogTerm,
                             LogEntry[] entries, long commitIndex)
    {
        return 0;
    }

    public int RequestVote(long term, int candidateId, long lastIndex, long lastLogTerm)
    {
        if(node.currentTerm>term ||( node.lastApplied>lastIndex))
        {
            return -1;
        }
        if(node.votedFor==0 || node.votedFor==candidateId)
        {
            node.votedFor = candidateId;
            return 0;
        }
        return 0;
    }

    class ElectionTimer{
        /**
         * The Rand.
         */
        Random rand= new Random();
        /**
         * The Random time.
         */
        int randomTime = (int) (node.leaderTimeout + rand.nextDouble()*150);
        /**
         * The Election.
         */
        ScheduledFuture election = electionTimer.schedule(new StartElection(),
                randomTime, TimeUnit.MILLISECONDS);


    }
    public static void main(String[] args){

    }
}
