package com.github.scaars10.pecanraft.server;


import com.github.scaars10.pecanraft.ClientRequest;
import com.github.scaars10.pecanraft.ClientResponse;
import com.github.scaars10.pecanraft.*;
import com.github.scaars10.pecanraft.structures.LogEntry;
import com.github.scaars10.pecanraft.utility.ResettableCountDownLatch;
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
    /**
      Consensus latch is used to make the leader wait before it sends response to client
        and verify if it is still a leader, it waits for a positive reply from
        majority of nodes in its AppendEntries routine.
     **/
    ResettableCountDownLatch consensusLatch = new ResettableCountDownLatch(1);

    PecanNode node;
    Server server;

    //method to start gRpc Server in a separate thread
    private void startServer()
    {
        server = ServerBuilder.forPort(PecanConfig.getPort(node.id)).
                addService(raftService).build();

        serverThread = new Thread(()->
        {
        try {
            server.start();
            System.out.println("--Server for node "+node.id+" started..");

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


    }


    public PecanServer(int id, PecanConfig config)
    {
        this.node = new PecanNode(id, config);
        this.raftService = new RaftServiceImpl(node);
        this.rpcClient = new RaftGrpcServiceClient(node);

        startServer();
        node.writeMessage("xx Server for node-"+id+" created");
        serverThread.start();

    }

    public void start()
    {
        System.out.println("Starting for node "+node.id);
        startFollower();
    }

    void stopLeader()
    {
        node.nodeLock.writeLock().lock();
        updateStatus(node.getCurrentTerm(), -1, -1);
        node.nodeLock.writeLock().unlock();
    }

    /** FIXME:
     *      Method to stop the server but it does not completely stop the server, probably because of
     *      some gRpc services still running. Only way to stop server as of now is to shut down the program..
     */
    public void stop()
    {
        stopLeader();
        server.shutdownNow();
        serverThread.interrupt();
        try {
            server.awaitTermination();
            System.out.println(server.getServices());
            System.out.println(server.isShutdown() || server.isTerminated());
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public boolean isRunning()
    {
        return server!=null && !(server.isShutdown() || server.isTerminated());
    }
    ScheduledExecutorService electionExecutor = Executors.newScheduledThreadPool(1);

    ScheduledFuture electionFuture;


    /**
     * Class for a thread to run in parallel when timer on the node times out and election starts.
     */

    void startLeader()
    {
        System.out.println("New Leader - "+node.id+" in term "+node.getCurrentTerm());
        while(true)
        {
            node.nodeLock.readLock().lock();
            PecanNode.possibleStates state = node.nodeState;
            node.nodeLock.readLock().unlock();
            if(state!= PecanNode.possibleStates.LEADER)
            {
                System.out.println("Server "+node.id+" no longer leader..");
                break;
            }
            new Thread(this::allAppendEntries).start();

            try {
                Thread.sleep(node.heartbeat);
                consensusLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }


    void allAppendEntries()
    {
        AtomicInteger successCount = new AtomicInteger(1);
        node.logLock.readLock().lock();
        long currentSize = node.getLogSize()-1;
        node.logLock.readLock().unlock();
        //System.out.println("Append Entry routine started for node "+node.id);
        new Thread(()->
        {
            List<Integer> peerList = new ArrayList<>();

            for (int i = 0; i < node.peerId.length; i++) {
                if (i != node.id)
                    peerList.add(i);
            }
            peerList.stream().parallel().forEach((peerId) ->
                    rpcClient.appendEntries("localhost", peerId, successCount));
        }).start();

        for(int i=0;i<20;i++)
        {
            try {
                Thread.sleep(40);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(successCount.get()> node.peerId.length/2)
            {
                node.nodeLock.writeLock().lock();
                node.setCommitIndex(currentSize);
                node.nodeLock.writeLock().unlock();
            }

        }


    }


    void startFollower()
    {
        startElectionTimer();
    }
    void startElection()
    {
        //Startup Election
        System.out.println("For Node "+node.id+" Timed out. No heartbeat received from leader with " +
                "(id-"+node.leaderId+"). Starting a new election");

        //obtain a writeLock to ensure safety
        node.nodeLock.writeLock().lock();
        //change the state to candidate
        node.nodeState = PecanNode.possibleStates.CANDIDATE;
        node.setVotedFor(node.id);
        node.setCurrentTerm(node.getCurrentTerm()+1);
        long currentTerm = node.getCurrentTerm();
        //System.out.println("For node "+node.id+" current term "+node.getCurrentTerm());
        AtomicInteger voteCount = new AtomicInteger(1);
        //release the writeLock
        node.nodeLock.writeLock().unlock();

        final ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);

        //create a list of Futures, so you know when each tick is done
        final List<Future> futures = new ArrayList<>();

        for(int i=0;i<node.peerId.length;i++)
        {
            restartElectionTimer();
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
                    else if(res.getTerm()>currentTerm)
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
                /* FIXME:
                 *  This line of code sometimes throws InterruptedException. Cause is unknown as of now
                 */
                Thread.sleep(20);
            } catch (InterruptedException e)
            {
                System.out.println("Election Sleep interrupted for "+node.id);
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
            node.nodeLock.readLock().lock();
            PecanNode.possibleStates state = node.nodeState;
            node.nodeLock.readLock().unlock();
            //stop waiting if a new leader was elected meanwhile or this candidate has obtained majority of votes
            if(state== PecanNode.possibleStates.FOLLOWER ||
                    voteCount.get()>(node.peerId.length-1)/2)
            {
                break;
            }

        }
        System.out.println("Node "+node.id+" received "+voteCount.get()+" votes in term "+node.getCurrentTerm());
        if(voteCount.get()>(node.peerId.length-1)/2)
        {
            System.out.println("Node "+node.id+" has won the election for term "+node.getCurrentTerm());
            node.nodeState = PecanNode.possibleStates.LEADER;
            startLeader();

        }


    }

    LogEntry getDummyLog()
    {

        return new LogEntry(-1, -1, -1, -1);

    }
    void  startElectionTimer()
    {
        //System.out.println("Election timer for node "+node.id+" started");
        Random rand= new Random();
        int randomTime = (int) (node.leaderTimeout + rand.nextDouble()*150);
        electionFuture = electionExecutor.schedule((this::startElection), randomTime, TimeUnit.MILLISECONDS);


    }
    void restartElectionTimer()
    {
        stopElectionTimer();
        startElectionTimer();
    }
    void stopElectionTimer()
    {
        if(electionFuture!=null && !(electionFuture.isDone() || electionFuture.isCancelled()))
            electionFuture.cancel(true);

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

    public boolean isDummyLog(RpcLogEntry log)
    {
        return log.getIndex() == -1;
    }

    public class RaftServiceImpl extends RaftNodeRpcGrpc.RaftNodeRpcImplBase {
        PecanNode node;
        RaftServiceImpl(PecanNode node){
            this.node = node;
        }

        //returns true if the server making the request is behind
        boolean checkIfServerIsBehind(long term, long lastIndex, long lastLogTerm)
        {
            LogEntry lastLog = node.getLastLog();
            if(lastLog == null)
            {
                lastLog = new LogEntry(-1, -1, -1, -1);
            }
            return node.getCurrentTerm() > term || ((node.getCommitIndex() > lastIndex) &&
                    (lastLog.getTerm() >= lastLogTerm));
        }


        @Override
        public StreamObserver<AppendEntriesRequest> appendEntries
                (StreamObserver<AppendEntriesResponse> responseObserver)
        {

            return new StreamObserver<AppendEntriesRequest>() {
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
                        System.out.println("For node"+node.id+" AE failed from "+value.getLeaderId());
                        System.out.println("CT - "+node.getCurrentTerm()+ "LT - "+value.getTerm());
                        AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                            .setResponseCode(AppendEntriesResponse.ResponseCodes.OUTDATED)
                            .setTerm(node.getCurrentTerm()).build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                    else
                    {
                        updateStatus(value.getTerm(), value.getLeaderId(), -1);

                        restartElectionTimer();
                        if (value.getLogEntriesCount() == 0 || isDummyLog(value.getLogEntriesList().get(0)))
                        {
                            AppendEntriesResponse response = AppendEntriesResponse.newBuilder()
                                    .setResponseCode(AppendEntriesResponse.ResponseCodes.SUCCESS).build();
                            responseObserver.onNext(response);
                            responseObserver.onCompleted();
                        }
                        else
                        {
                            //System.out.println("F "+value.getLogEntriesCount());
                            List <RpcLogEntry> aeLogs = value.getLogEntriesList();
                            RpcLogEntry firstLog = aeLogs.get(0);
                            LogEntry resultLog = node.getLog(firstLog.getIndex());
                            if(resultLog==null && firstLog.getIndex()==0)
                            {
                                AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                        setResponseCode(AppendEntriesResponse.ResponseCodes.SUCCESS).build();
                                responseObserver.onNext(response);
                                responseObserver.onCompleted();

                                System.out.println("Writing to Db..");
                                new Thread(() ->
                                        node.updateUncommittedLog(value.getLogEntriesList(),
                                                0)).start();
                            }

                            else if(resultLog!=null && resultLog.getTerm()==firstLog.getTerm())
                            {
                                AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                   setResponseCode(AppendEntriesResponse.ResponseCodes.SUCCESS).build();
                                responseObserver.onNext(response);
                                responseObserver.onCompleted();
                                RpcLogEntry lastRpcLog = value.getLogEntries(value.getLogEntriesCount()-1);
                                LogEntry lastLog = node.getLastLog();
                                //System.out.println("ll " + lastLog.getIndex()+" "+lastLog.getTerm());
                                //System.out.println("lr "+lastRpcLog.getIndex()+" "+lastRpcLog.getTerm());
                                if((lastLog.getTerm()!=lastRpcLog.getTerm()) ||
                                        (lastRpcLog.getIndex() != lastLog.getIndex()))
                                {
                                    System.out.println("Writing to Db..");
                                    node.updateUncommittedLog(value.getLogEntriesList(),
                                                resultLog.getIndex());

                                }
                            }
                            else
                            {
                                System.out.println("MORE");
                                AppendEntriesResponse response = AppendEntriesResponse.newBuilder().
                                        setResponseCode(AppendEntriesResponse.ResponseCodes.MORE)
                                        .setMatchIndex(node.getCommitIndex()).build();
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
                    //System.out.println("Released Locks");
                    node.logLock.writeLock().unlock();
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
        }

        @Override
        public void requestVote(com.github.scaars10.pecanraft.RequestVoteRequest request,
                StreamObserver<com.github.scaars10.pecanraft.RequestVoteResponse> responseObserver)
        {
            try
            {
                node.nodeLock.writeLock().lock();
                //System.out.println("Node " + node.id + " received vote request from " + request.getCandidateId());
                long candidateTerm = request.getTerm(), lastLogIndex = request.getLastLogIndex(),
                        lastLogTerm = request.getLastLogTerm();
                int candidateId = request.getCandidateId();
                com.github.scaars10.pecanraft.RequestVoteResponse response;

                if (node.getCurrentTerm() < request.getTerm()) {
                    updateStatus(request.getTerm(), node.leaderId, -1);
                }

                if (((node.getVotedFor() == candidateId) || (node.getVotedFor() == -1))
                        && (!checkIfServerIsBehind(candidateTerm, lastLogIndex, lastLogTerm))) {
                    response = RequestVoteResponse.newBuilder().setVoteGranted(true).build();
                    restartElectionTimer();
                   /* System.out.println("Node " + node.id + " voted for node " + request.getCandidateId()
                            + " in term " + node.getCurrentTerm());
                            */
                    updateStatus(request.getTerm(), request.getCandidateId(), request.getCandidateId());

                } else {
                    response = RequestVoteResponse.newBuilder().setTerm(node.getCurrentTerm()).
                            setVoteGranted(false).build();

                }
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            finally {
                node.nodeLock.writeLock().unlock();
            }
        }

        @Override
        public void systemService(ClientRequest request, StreamObserver<ClientResponse> responseObserver) {
            ClientResponse response;
            System.out.println("Server "+node.id+" received client request..");
            node.nodeLock.readLock().lock();
            PecanNode.possibleStates state = node.nodeState;
            node.nodeLock.readLock().unlock();
            if(state != PecanNode.possibleStates.LEADER)
            {
                response = ClientResponse.newBuilder().setLeaderId(node.leaderId)
                .setSuccess(false).build();
            }

            else
            {
                boolean result = false;


                try {
                    result = consensusLatch.await(1000,TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                finally {
                    consensusLatch.reset();
                }
                node.nodeLock.readLock().lock();
                state = node.nodeState;
                node.nodeLock.readLock().unlock();
                if(!result || state != PecanNode.possibleStates.LEADER)
                {
                    response = ClientResponse.newBuilder()
                            .setSuccess(false).build();
                }
                else
                {
                    response = ClientResponse.newBuilder()
                            .setSuccess(true).build();
                    int key = request.getKey();
                    int value = request.getValue();
                    node.nodeLock.writeLock().lock();
                    node.addToUncommittedLog(key, value);
                    node.nodeLock.writeLock().unlock();
                    System.out.println("Handled Request successfully.. ");
                }


            }
            System.out.println("Handled Request.. ");
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
            try {

                RaftNodeRpcGrpc.RaftNodeRpcBlockingStub client = RaftNodeRpcGrpc.newBlockingStub(channel);
                node.logLock.readLock().lock();
                LogEntry lastLog = node.getLastCommittedLog();
                node.logLock.readLock().unlock();
                if (lastLog == null) {
                    lastLog = new LogEntry(-1, -1, -1, -1);
                }
                RequestVoteRequest req = RequestVoteRequest.newBuilder().
                        setCandidateId(node.id).setTerm(node.getCurrentTerm()).setLastLogIndex(lastLog.getIndex()).
                        setLastLogTerm(lastLog.getTerm()).build();
                return client.requestVote(req);
            }
            finally {
                channel.shutdown();
            }

        }

        public void appendEntries(String address, int nodeId, AtomicInteger successCount)
        {
            CountDownLatch latch = new CountDownLatch(1);
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(address, PecanConfig.getPort(nodeId))
                    .usePlaintext()
                    .build();
            //System.out.println("Append Entries req sent to "+nodeId+ "from "+node.id);
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
                        node.nodeLock.writeLock().lock();
                        updateStatus(value.getTerm(),-1,-1);
                        node.nodeLock.writeLock().unlock();
                    }
                    if(value.getResponseCode() == AppendEntriesResponse.ResponseCodes.MORE)
                    {
                        long index = value.getMatchIndex();

                        node.logLock.readLock().lock();
                        List<LogEntry> logs = node.getLogs((int) index, -1);
                        LogEntry lastLog = node.getLastLog();


                        List <RpcLogEntry> rpcLogs = new ArrayList<>();
                        logs.forEach(log-> rpcLogs.add(RpcLogEntry.newBuilder()
                                .setIndex(log.getIndex()).setTerm(log.getTerm())
                                .setKey(log.getKey()).setValue(log.getValue())
                                .build()));

                        if(lastLog == null)
                        {
                            lastLog = new LogEntry(-1, -1, -1, -1);
                        }
                        node.nodeLock.readLock().lock();
                        AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                                .addAllLogEntries(rpcLogs).setPrevLogTerm(lastLog.getTerm())
                                .setPrevLogIndex(lastLog.getIndex()).setTerm(node.getCurrentTerm())
                                .setLeaderId(node.id).setCommitIndex(node.getCommitIndex())
                                .build();
                        node.nodeLock.readLock().unlock();
                        node.logLock.readLock().unlock();
                        requestObserverRef.get().onNext(req);
                    }
                    if(value.getResponseCode()== AppendEntriesResponse.ResponseCodes.SUCCESS)
                    {
                        successCount.incrementAndGet();
                        node.nextIndex[nodeId] =
                                node.getLogSize();
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

            node.logLock.readLock().lock();
            LogEntry lastLog = node.getLastLog();
            List<LogEntry> logs = node.getLogs((int)
                    Math.min(node.nextIndex[nodeId]-1, node.getCommitIndex()), -1);
            node.logLock.readLock().unlock();

            if(lastLog == null)
            {
                lastLog = new LogEntry(-1, -1, -1, -1);
            }

            List <RpcLogEntry> rpcLogs = new ArrayList<>();
            if(logs==null)
            {
                logs= new ArrayList<>();
                logs.add(getDummyLog());
            }
            logs.forEach(log-> rpcLogs.add(RpcLogEntry.newBuilder()
                    .setIndex(log.getIndex()).setTerm(log.getTerm())
                    .setKey(log.getKey()).setValue(log.getValue())
                    .build()));
            requestObserverRef.get().onNext(AppendEntriesRequest
                    .newBuilder().setLeaderId(node.id).setCommitIndex(node.getCommitIndex())
                    .setPrevLogIndex(lastLog.getIndex()).setTerm(node.getCurrentTerm())
                            .setPrevLogTerm(lastLog.getTerm())
                    .addAllLogEntries(rpcLogs)
                    .build());
            try {
                latch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                node.logError(e.getMessage());
            }
            channel.shutdown();

        }

    }
}
