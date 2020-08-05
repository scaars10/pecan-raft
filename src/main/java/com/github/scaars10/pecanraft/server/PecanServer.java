package com.github.scaars10.pecanraft.server;


import com.github.scaars10.pecanraft.ClientRequest;
import com.github.scaars10.pecanraft.ClientResponse;
import com.github.scaars10.pecanraft.*;
import com.github.scaars10.pecanraft.structures.LogEntry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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


    public PecanServer(int id, int[]peerId)
    {
        this.node = new PecanNode(id, peerId);
        this.raftService = new RaftServiceImpl(node);
        this.rpcClient = new RaftGrpcServiceClient(node);

        startServer();
        node.writeMessage("Server for node-"+id+" created");
    }


    final ExecutorService electionExecutor = Executors.newFixedThreadPool(1);
    Future<?> electionFuture;

    final ScheduledExecutorService leaderExecutor = Executors.newScheduledThreadPool
            (2);
    Future<?> leaderFuture;

    /**
     * Class for a thread to run in parallel when timer on the node times out and election starts.
     */


    void startLeader()
    {
        while(node.nodeState == PecanNode.possibleStates.LEADER)
        {
            try {
                Thread.sleep(node.heartbeat);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            allAppendEntries();

        }
    }

    void stopLeader()
    {

        leaderFuture.cancel(true);
    }

    void allAppendEntries()
    {

        new Thread(()->
        {
            List<Integer> peerList = new ArrayList<>();

            for (int i = 0; i < node.peerId.length; i++) {
                if (i != node.id)
                    peerList.add(i);
            }
            peerList.stream().parallel().forEach((peerId) ->
            {
                rpcClient.appendEntries("localhost", peerId);
            });
        }).start();


    }
    void startFollower()
    {

        startElectionTimer();
    }
    void startElection()
    {
        //Startup Election
        node.writeMessage("Timed out. No heartbeat received from leader with " +
                "(id-"+node.leaderId+"). Starting a new election");

        //obtain a writeLock to ensure safety
        node.nodeLock.writeLock().lock();
        //change the state to candidate
        node.nodeState = PecanNode.possibleStates.CANDIDATE;
        node.setVotedFor(node.id);
        node.setCurrentTerm(node.getCurrentTerm()+1);
        AtomicInteger voteCount = new AtomicInteger(1);
        //release the writeLock
        node.nodeLock.writeLock().unlock();

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
                    node.writeMessage("General Exception for - node-" + node.peerId[finalI]);
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
            if(node.nodeState== PecanNode.possibleStates.FOLLOWER ||
                    voteCount.get()>(node.peerId.length-1)/2)
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

    long max_of(long a, long b)
    {
        if(a>b)
            return a;
        return b;
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

    public void updateStatus(long term, int leaderId, int newvotedFor)
    {
        node.setCurrentTerm(term);
        node.nodeState = PecanNode.possibleStates.FOLLOWER;
        node.leaderId = leaderId;
        if(newvotedFor>=0)
            node.setVotedFor(newvotedFor);
        else
            node.setVotedFor(-1);
    }


    public class RaftServiceImpl extends RaftNodeRpcGrpc.RaftNodeRpcImplBase {
        PecanNode node;
        RaftServiceImpl(PecanNode node){
            this.node = node;
        }

        //returns true if the server making the request is behind
        boolean checkIfServerIsBehind(long term, long lastIndex, long lastLogTerm)
        {
            return node.getCurrentTerm() > term || ((node.getCommitIndex() > lastIndex) &&
                    (node.getLastCommittedLog().
                            getTerm() >= lastLogTerm));
        }


        @Override
        public StreamObserver<AppendEntriesRequest> appendEntries
                (StreamObserver<AppendEntriesResponse> responseObserver)
        {
            StreamObserver <AppendEntriesRequest> streamObserver =
                new StreamObserver<AppendEntriesRequest>() {
                @Override
                public void onNext(AppendEntriesRequest value)
                {
                    try
                    {
                        node.logLock.writeLock().lock();
                        node.nodeLock.writeLock().lock();
                        long term = value.getTerm();
                        if(node.getCurrentTerm()>term)
                        {
                            AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                                .setResponseCode(AppendEntriesResponse.ResponseCodes.OUTDATED)
                                .setTerm(node.getCurrentTerm()).build();
                            responseObserver.onNext(response);
                            responseObserver.onCompleted();
                        }
                        else {
                            restartElectionTimer();
                            if (value.getLogEntriesCount() == 0) {
                                responseObserver.onCompleted();
                            } else {
                                long nodeMatchIndex = node.getCommitIndex()
                                        , leaderMatchIndex = node.getCommitIndex();
                                LogEntry searchLog;
                                for (RpcLogEntry log : value.getLogEntriesList())
                                {
                                    long logIndex = log.getIndex(), logTerm = log.getTerm();
                                    searchLog = node.getLog(logIndex);
                                    if (searchLog == null || searchLog.getTerm() != logTerm) {

                                        break;

                                    } else if (searchLog.getTerm() == logTerm) {
                                        leaderMatchIndex = logIndex;
                                        nodeMatchIndex = searchLog.getIndex();
                                    }

                                }
                                if (nodeMatchIndex > node.getCommitIndex()) {
                                    AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                        setResponseCode(AppendEntriesResponse.ResponseCodes.SUCCESS).
                                        setMatchIndex(nodeMatchIndex).build();
                                    responseObserver.onNext(response);
                                    responseObserver.onCompleted();
                                    long finalLeaderMatchIndex = leaderMatchIndex;
                                    long finalNodeMatchIndex = nodeMatchIndex;
                                    Thread dbThread = new Thread(()->
                                    {
                                        node.updateUncommittedLog(value.getLogEntriesList().subList
                                                        ((int) finalLeaderMatchIndex, value.getLogEntriesCount()),
                                                finalNodeMatchIndex, finalLeaderMatchIndex);

                                        node.setCommitIndex(value.getCommitIndex());
                                    });
                                    dbThread.start();



                                } else {
                                    AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                            setResponseCode(AppendEntriesResponse.ResponseCodes.MORE).
                                            setMatchIndex(nodeMatchIndex).build();
                                    responseObserver.onNext(response);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        node.logError(e.getMessage());
                    }
                    finally
                    {
                        if(node.logLock.writeLock().isHeldByCurrentThread())
                            node.logLock.writeLock().unlock();

                        if(node.nodeLock.writeLock().isHeldByCurrentThread())
                            node.nodeLock.writeLock().unlock();
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

            if(node.getCurrentTerm()<request.getTerm())
            {
                updateStatus(request.getTerm(), node.leaderId, -1);
            }

            if(((node.getVotedFor()==candidateId) || (node.getVotedFor()==-1))
             && (!checkIfServerIsBehind(candidateTerm, lastLogIndex, lastLogTerm)))
            {
                response = RequestVoteResponse.newBuilder().setVoteGranted(true).build();
                updateStatus(request.getTerm(), request.getCandidateId(), request.getCandidateId());

            }
            else
            {
                response = RequestVoteResponse.newBuilder().setTerm(node.getCurrentTerm()).
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

        public RequestVoteResponse requestVote(String address, int nodeId)
        {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(address, PecanConfig.getPort(nodeId))
                    .usePlaintext()
                    .build();

            RaftNodeRpcGrpc.RaftNodeRpcBlockingStub client = RaftNodeRpcGrpc.newBlockingStub(channel);
            LogEntry lastLog = node.getLastCommittedLog();
            RequestVoteRequest req = RequestVoteRequest.newBuilder().
                    setCandidateId(node.id).setTerm(node.getCurrentTerm()).setLastLogIndex(lastLog.getIndex()).
                    setLastLogTerm(lastLog.getTerm()).build();
            return client.requestVote(req);
        }

        public void appendEntries(String address, int nodeId)
        {
            CountDownLatch latch = new CountDownLatch(1);
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(address, PecanConfig.getPort(nodeId))
                    .usePlaintext()
                    .build();

            RaftNodeRpcGrpc.RaftNodeRpcStub client = RaftNodeRpcGrpc.newStub(channel);
            AtomicReference<StreamObserver<AppendEntriesRequest>> requestObserverRef =
                    new AtomicReference<>();
            StreamObserver<AppendEntriesRequest> streamObserver =
            client.appendEntries(new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(AppendEntriesResponse value)
                {
                    if(value.getResponseCode() == AppendEntriesResponse.ResponseCodes.OUTDATED)
                    {
                        updateStatus(value.getTerm(),-1,-1);
                    }
                    if(value.getResponseCode() == AppendEntriesResponse.ResponseCodes.MORE)
                    {
                        long index = value.getMatchIndex();
                        List<LogEntry> logs = node.getLogs((int) index, -1);
                        List <RpcLogEntry> rpcLogs = new ArrayList<>();
                        logs.forEach(log->{
                            rpcLogs.add(RpcLogEntry.newBuilder()
                                    .setIndex(log.getIndex()).setTerm(log.getTerm())
                                    .setKey(log.getKey()).setValue(log.getValue())
                                    .build());
                        });

                        LogEntry lastLog = node.getLastLog();

                        AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                                .addAllLogEntries(rpcLogs).setPrevLogTerm(lastLog.getTerm())
                                .setPrevLogIndex(lastLog.getIndex())
                                .setLeaderId(node.id).setCommitIndex(node.getCommitIndex())
                                .build();
                        requestObserverRef.get().onNext(req);
                    }

                }

                @Override
                public void onError(Throwable t)
                {
                    latch.countDown();
                }

                @Override
                public void onCompleted()
                {
                    latch.countDown();
                }
            });
            requestObserverRef.set(streamObserver);
            LogEntry lastLog = node.getLastLog();
            List<LogEntry> logs = node.getLogs((int)
                    node.nextIndex[(int) max_of(nodeId, node.getCommitIndex())], -1);
            List <RpcLogEntry> rpcLogs = new ArrayList<>();
            logs.forEach(log->{
                rpcLogs.add(RpcLogEntry.newBuilder()
                        .setIndex(log.getIndex()).setTerm(log.getTerm())
                        .setKey(log.getKey()).setValue(log.getValue())
                        .build());
            });
            requestObserverRef.get().onNext(AppendEntriesRequest
                    .newBuilder().setLeaderId(node.id).setCommitIndex(node.getCommitIndex())
                    .setPrevLogIndex(lastLog.getIndex()).
                            setPrevLogTerm(lastLog.getTerm())
                    .addAllLogEntries(rpcLogs)
                    .build());
            try {
                latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                node.logError(e.getMessage());
            }

        }

    }
}
