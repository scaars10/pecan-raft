package com.github.scaars10.pecanraft.server;

import java.util.*;

public class PecanConfig {

    static Map <Integer, Integer> idToPort = new HashMap<>();
    int num = 3;
    int []peerId;

    public PecanConfig()
    {
        peerId = new int[num];
        for(int i=0;i<num;i++)
        {
            peerId[i] = i;
        }
        buildMap();
    }
    public void buildMap()
    {
        for(int i=0;i<peerId.length;i++)
        {
            idToPort.put(i, 50080+i);
        }
    }
    public int getNumServer()
    {
        return num;
    }
    public static int getPort(int id)
    {
        return idToPort.get(id);
    }

    public  int[] getPeerId()
    {
        return peerId;
    }

    public static Map<Integer, Integer> getMap()
    {
        return idToPort;
    }

}
