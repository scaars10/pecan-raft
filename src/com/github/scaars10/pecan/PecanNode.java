package com.github.scaars10.pecan;

import com.github.scaars10.pecan.RMI.ClientRmiClass;
import com.github.scaars10.pecan.RMI.ServerRmiClass;

import java.util.Random;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The type Pecan node.
 */
public class PecanNode {


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
    int leaderId;
    /**
     * Current term according to this node
     */
    long currentTerm = 0;
    /**
     * The Election timer.
     */
    ScheduledExecutorService electionTimer =
            Executors.newScheduledThreadPool(1);

    /**
     * Stores the ids of peers
     */
    int []peerId;
    /**
     * Initial Node state. All nodes are followers in the beginning.
     */
    possibleStates nodeState = possibleStates.FOLLOWER;

    /**
     * Instantiates a new Pecan node.
     *
     * @param id     the id
     * @param peerId the peer id
     */
    ClientRmiClass clientRaft;
    ServerRmiClass serverRaft;
    PecanNode(int id, int []peerId){
        this.id = id;
        this.peerId = peerId;
    }

    /**
     * Class for a thread to run in parallel when timer on the node times out and election starts.
     */

    class StartElection implements Runnable{
        public void run(){
            System.out.println("Timed out. No heartbeat received. Starting a new election");
            nodeState = possibleStates.CANDIDATE;
            //Startup Election
        }
    }

    /**
     * The type Heartbeat timer.
     */
    class ElectionTimer{
        /**
         * The Rand.
         */
        Random rand= new Random();
        /**
         * The Random time.
         */
        int randomTime = (int) (150 + rand.nextDouble()*150);
        /**
         * The Election.
         */
        ScheduledFuture election = electionTimer.schedule(new StartElection(),
                randomTime, TimeUnit.MILLISECONDS);
    }


}
