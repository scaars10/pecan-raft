package com.github.scaars10.pecanraft.client;

import com.github.scaars10.pecanraft.ClientRequest;
import com.github.scaars10.pecanraft.RaftNodeRpcGrpc;
import com.github.scaars10.pecanraft.server.PecanConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

// Class for manual testing

public class TestClient
{




    public static void main(String[] args) {
        PecanConfig config = new PecanConfig();
        ManagedChannel channel1 = ManagedChannelBuilder
                .forAddress("localhost", PecanConfig.getPort(1))
                .usePlaintext()
                .build();
        ManagedChannel channel0 = ManagedChannelBuilder
                .forAddress("localhost", PecanConfig.getPort(0))
                .usePlaintext()
                .build();
        ManagedChannel channel2 = ManagedChannelBuilder
                .forAddress("localhost", PecanConfig.getPort(2))
                .usePlaintext()
                .build();

        RaftNodeRpcGrpc.RaftNodeRpcBlockingStub client[] = new RaftNodeRpcGrpc.RaftNodeRpcBlockingStub[3];
        client[0] = RaftNodeRpcGrpc.newBlockingStub(channel0);
        client[1] = RaftNodeRpcGrpc.newBlockingStub(channel1);
        client[2] = RaftNodeRpcGrpc.newBlockingStub(channel2);
        Scanner in = new Scanner(System.in);
        while(true) {
            String input = in.nextLine();
            String[] arr = input.split(" ");
            int clientId = Integer.parseInt(arr[0]), key = Integer.parseInt(arr[1]) , value = Integer.parseInt( arr[2]);
            ClientRequest req = ClientRequest.newBuilder().setKey(key).setValue(value).build();
            client[clientId].systemService(req);
        }
    }
}
