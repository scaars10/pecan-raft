package com.github.scaars10.pecanraft.Server;



import com.github.scaars10.pecanRaft.ClientRequest;
import com.github.scaars10.pecanRaft.ClientResponse;
import com.github.scaars10.pecanraft.*;
import com.github.scaars10.pecanraft.AppendEntriesRequest;
import com.github.scaars10.pecanraft.AppendEntriesResponse;
import com.github.scaars10.pecanraft.RequestVoteResponse;
import com.github.scaars10.pecanraft.RpcLogEntry;
import com.github.scaars10.pecanraft.structures.LogEntry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PecanServer {
    RaftServiceImpl raftService;
    Thread serverThread;

    PecanNode node;

    //method to start gRpc Server in a separate thread
    private void startServer()
    {
        Server server = ServerBuilder.forPort(PecanConfig.getPort(node.id)).
                addService(raftService).build();

        serverThread = new Thread(()->
        {
            try {
                server.start();
                System.out.println("Server for node "+node.id+" started..");

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("Received Shutdown Request");
                    server.shutdown();
                    System.out.println("Successfully stopped the server");
                }));

                server.awaitTermination();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        serverThread.start();
    }


    public PecanServer(int id, int[]peerId){


        this.node = new PecanNode(id, peerId);
        this.raftService = new RaftServiceImpl(node);

        startServer();
        System.out.println("Object for node"+id+" created");
    }


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

        //obtain a writeLock to ensure safety
        node.nodeLock.writeLock().lock();
        //change the state to candidate
        node.nodeState = PecanNode.possibleStates.CANDIDATE;
        node.votedFor.set(node.id);
        node.currentTerm++;
        AtomicInteger voteCount = new AtomicInteger(1);
        long lastLogTerm = 0;
        if(node.committedLog.size()>0){
            lastLogTerm = node.committedLog.get(node.committedLog.size()-1).getTerm();
        }
        //release the writeLock
        node.nodeLock.writeLock().lock();

        final ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);

        //create a list of Futures, so you know when each tick is done
        final List<Future> futures = new ArrayList<>();
        for(int i=0;i<node.peerId.length;i++)
        {
            if(node.peerId[i]==node.id)
                continue;

            int finalI = i;
            long finalLastLogTerm = lastLogTerm;
            final Future<?> future = exec.submit(() -> {

                try {
                    int response = 0;
                    //Request vote from peer [i]
//                    RmiInterface peer = (RmiInterface) Naming.lookup("Node-" + node.peerId[finalI]);
//
//                    int response = peer.RequestVote(node.currentTerm, node.id, node.commitIndex, finalLastLogTerm);
                    if (response == 0) {
                        voteCount.incrementAndGet();
                    }
                }
                //catching Exceptions
                catch (Exception e) {
                    System.out.println("General Exception for - node-" + node.peerId[finalI]);
                    e.printStackTrace();
                }
            });
            futures.add(future);

        }
        for(int i=0;i<50;i++)
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






//    @Override
//    public int AppendEntries(long term, int leaderId, long prevLogIndex, long prevLogTerm,
//                             LogEntry[] entries, long commitIndex)
//    {
//        node.nodeLock.readLock().lock();
//        if(checkIfServerIsBehind(term, prevLogIndex, prevLogTerm))
//        {
//            node.nodeLock.readLock().unlock();
//            return -1;
//        }
//        return 0;
//    }

//    public int RequestVote(long term, int candidateId, long prevLogIndex, long prevLogTerm)
//    {
//        node.nodeLock.writeLock().lock();
//        int res = -1;
//        if(node.votedFor.get()==0 || node.votedFor.get()==candidateId || !checkIfServerIsBehind(term, prevLogIndex, prevLogTerm))
//        {
//            node.votedFor.set(candidateId);
//            res = 0;
//        }
//        node.nodeLock.writeLock().unlock();
//        if(res==0)
//        {
//            stopElectionTimer();
//            startElectionTimer();
//        }
//        return res;
//    }

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
    void restartElectionTimer()
    {
        stopElectionTimer();
        startElectionTimer();
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

    public class RaftServiceImpl extends RaftNodeRpcGrpc.RaftNodeRpcImplBase {
        PecanNode node;
        RaftServiceImpl(PecanNode node){
            this.node = node;
        }

        //returns true if the server making the request is behind
        boolean checkIfServerIsBehind(long term, long lastIndex, long lastLogTerm)
        {
            return node.currentTerm > term || ((node.lastApplied > lastIndex) &&
                    (node.committedLog.get(node.committedLog.size()-1).
                            getTerm() >= lastLogTerm));
        }

        @Override
        public StreamObserver<AppendEntriesRequest> appendEntries
                (StreamObserver<com.github.scaars10.pecanraft.AppendEntriesResponse> responseObserver) {
            StreamObserver <AppendEntriesRequest> streamObserver = new StreamObserver<AppendEntriesRequest>() {
                @Override
                public void onNext(AppendEntriesRequest value) {
                    restartElectionTimer();
                    long term = value.getTerm();
                    if(node.currentTerm>term)
                    {
                        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                                .setResponseCode(AppendEntriesResponse.ResponseCodes.OUTDATED).
                                setTerm(node.currentTerm).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                    else
                    {
                        restartElectionTimer();
                        if(value.getLogEntriesCount()==0)
                        {
                            responseObserver.onCompleted();
                        }
                        else
                        {
                            long matchIndex = node.commitIndex;
                            LogEntry searchLog;
                            for (RpcLogEntry log : value.getLogEntriesList())
                            {
                                long logIndex = log.getIndex(), logTerm = log.getTerm();
                                searchLog = node.getLog(logIndex);
                                if (searchLog == null || searchLog.getTerm() != logTerm)
                                {

                                    break;

                                } else if (searchLog.getTerm() == logTerm) {
                                    matchIndex = logIndex;
                                }

                            }
                            if(matchIndex>node.commitIndex)
                            {
                                AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                        setResponseCode(AppendEntriesResponse.ResponseCodes.SUCCESS).
                                        setMatchIndex(matchIndex).build();
                                responseObserver.onNext(response);
                                responseObserver.onCompleted();
                                node.updateUncommittedLog(value.getLogEntriesList(), matchIndex);
                                node.persistToDb(value.getCommitIndex());
                                node.commitIndex = value.getCommitIndex();
                            }
                            else
                            {
                                AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                        setResponseCode(AppendEntriesResponse.ResponseCodes.MORE).
                                        setMatchIndex(matchIndex).build();
                                responseObserver.onNext(response);
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted()
                {
                    
                }
            };
            return streamObserver;
        }

        @Override
        public void requestVote(com.github.scaars10.pecanraft.RequestVoteRequest request,
                StreamObserver<com.github.scaars10.pecanraft.RequestVoteResponse> responseObserver) {
            long candidateTerm = request.getTerm(), lastLogIndex = request.getLastLogIndex(),
            lastLogTerm = request.getLastLogTerm();
            int candidateId = request.getCandidateId();
            com.github.scaars10.pecanraft.RequestVoteResponse response;
            if(((node.votedFor.get()==candidateId) || (node.votedFor == null))
             && (checkIfServerIsBehind(candidateTerm, lastLogIndex, lastLogTerm)))
            {
                response = RequestVoteResponse.newBuilder().setVoteGranted(true).build();

            }
            else
            {
                response = RequestVoteResponse.newBuilder().setTerm(node.currentTerm).
                        setVoteGranted(false).build();
                return;
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            restartElectionTimer();
        }

        @Override
        public void systemService(ClientRequest request, StreamObserver<ClientResponse> responseObserver) {
            ClientResponse response;
            if(node.id != node.leaderId)
            {
                response = ClientResponse.newBuilder().setLeaderId(node.leaderId)
                .setSuccess(false).build();
            }
            else
            {
                response = ClientResponse.newBuilder()
                        .setSuccess(true).build();
                int key = request.getKey();
                int value = request.getValue();
                node.addToUncommittedLog(key, value);
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
