package com.github.scaars10.pecanraft.server;



import com.github.scaars10.pecanRaft.ClientRequest;
import com.github.scaars10.pecanRaft.ClientResponse;
import com.github.scaars10.pecanraft.*;
import com.github.scaars10.pecanraft.AppendEntriesRequest;
import com.github.scaars10.pecanraft.AppendEntriesResponse;
import com.github.scaars10.pecanraft.RequestVoteRequest;
import com.github.scaars10.pecanraft.RequestVoteResponse;
import com.github.scaars10.pecanraft.RpcLogEntry;
import com.github.scaars10.pecanraft.structures.LogEntry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PecanServer {
    RaftServiceImpl raftService;
    RaftGrpcServiceClient rpcClient;
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
        this.rpcClient = new RaftGrpcServiceClient(node);
        startServer();
        System.out.println("Object for node "+id+" created");
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

            final Future<?> future = exec.submit(() -> {

                try {


                    RequestVoteResponse res = rpcClient.requestVote("localhost", node.peerId[finalI]);
                    //Request vote from peer [i]
//                    RmiInterface peer = (RmiInterface) Naming.lookup("Node-" + node.peerId[finalI]);
//
//                    int response = peer.RequestVote(node.currentTerm, node.id, node.commitIndex, finalLastLogTerm);
                    if (res.getVoteGranted()) {
                        voteCount.incrementAndGet();
                    }
                    else
                    {
                        updateStatus(res.getTerm(), -1, -1);
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
        for(int i=0;i<70;i++)
        {
            try
            {
                Thread.sleep(20);
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

    public void updateStatus(long term, int leaderId, int votedFor)
    {
        node.currentTerm = term;
        node.nodeState = PecanNode.possibleStates.FOLLOWER;
        node.leaderId = leaderId;
        if(votedFor>=0)
            node.votedFor.set(votedFor);
        else
            node.votedFor = null;
    }


    public class RaftServiceImpl extends RaftNodeRpcGrpc.RaftNodeRpcImplBase {
        PecanNode node;
        RaftServiceImpl(PecanNode node){
            this.node = node;
        }

        //returns true if the server making the request is behind
        boolean checkIfServerIsBehind(long term, long lastIndex, long lastLogTerm)
        {
            return node.currentTerm > term || ((node.commitIndex > lastIndex) &&
                    (node.committedLog.get(node.committedLog.size()-1).
                            getTerm() >= lastLogTerm));
        }

        @Override
        public StreamObserver<AppendEntriesRequest> appendEntries
                (StreamObserver<com.github.scaars10.pecanraft.AppendEntriesResponse> responseObserver) {
            StreamObserver <AppendEntriesRequest> streamObserver = new StreamObserver<AppendEntriesRequest>() {
                @Override
                public void onNext(AppendEntriesRequest value) {

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
                StreamObserver<com.github.scaars10.pecanraft.RequestVoteResponse> responseObserver)
        {
            long candidateTerm = request.getTerm(), lastLogIndex = request.getLastLogIndex(),
            lastLogTerm = request.getLastLogTerm();
            int candidateId = request.getCandidateId();
            com.github.scaars10.pecanraft.RequestVoteResponse response;

            if(node.currentTerm<request.getTerm())
            {
                updateStatus(request.getTerm(), node.leaderId, -1);
            }

            if(((node.votedFor.get()==candidateId) || (node.votedFor == null))
             && (!checkIfServerIsBehind(candidateTerm, lastLogIndex, lastLogTerm)))
            {
                response = RequestVoteResponse.newBuilder().setVoteGranted(true).build();
                updateStatus(request.getTerm(), request.getCandidateId(), request.getCandidateId());

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

    public class RaftGrpcServiceClient
    {
        PecanNode node;
        public RaftGrpcServiceClient(PecanNode node)
        {
            this.node = node;
        }

        public RequestVoteResponse requestVote(String address, int port)
        {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port)
                    .usePlaintext()
                    .build();

            RaftNodeRpcGrpc.RaftNodeRpcBlockingStub client = RaftNodeRpcGrpc.newBlockingStub(channel);
            LogEntry lastLog = node.getLastCommittedLog();
            RequestVoteRequest req = RequestVoteRequest.newBuilder().
                    setCandidateId(node.id).setTerm(node.currentTerm).setLastLogIndex(lastLog.getIndex()).
                    setLastLogTerm(lastLog.getTerm()).build();
            return client.requestVote(req);

        }
    }
}
