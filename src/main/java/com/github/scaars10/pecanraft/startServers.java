package com.github.scaars10.pecanraft;

import com.github.scaars10.pecanraft.server.PecanConfig;
import com.github.scaars10.pecanraft.server.PecanServer;

public class startServers
{
    public static void main(String[] args)
    {

        int num = 3;
        PecanConfig config = new PecanConfig(num);
        PecanServer [] servers = new PecanServer[num];
        for(int i=0;i<num;i++)
        {
            servers[i] = new PecanServer(i, config);
        }
        System.out.println("Starting Servers...");
        for(int i=0;i<num;i++) {
            int finalI = i;
            new Thread(()->servers[finalI].start()).start();
        }
    }
}
