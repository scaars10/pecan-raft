package com.github.scaars10.pecanraft.dirTest;

import com.github.scaars10.pecanraft.server.PecanConfig;
import com.github.scaars10.pecanraft.server.PecanServer;

// Class for manual testing

public class startServer2
{
    public static void main(String[] args)
    {

        int num = 3;
        PecanConfig config = new PecanConfig();
        PecanServer server = new PecanServer(2, config);
        server.start();



    }
}
