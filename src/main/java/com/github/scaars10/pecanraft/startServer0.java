package com.github.scaars10.pecanraft;

import com.github.scaars10.pecanraft.server.PecanConfig;
import com.github.scaars10.pecanraft.server.PecanServer;

// Class for manual testing

public class startServer0
{
    public static void main(String[] args)
    {

        int num = 3;
        PecanConfig config = new PecanConfig();
        PecanServer server = new PecanServer(0, config);
        server.start();



    }
}
