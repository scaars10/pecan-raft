package com.github.scaars10.pecanraft.server;

import java.util.*;

public class PecanConfig {

    static Map <Integer, Integer> idToPort = new HashMap<>();

    int []portId = {50080,50081,50082,50083,50084,50085,50086};
    public static void buildMap()
    {
        for(int i=0;i<7;i++)
        {
            idToPort.put(i, 50080+i);
        }
    }
    public static int getPort(int id)
    {
        return idToPort.get(id);
    }

    public static Map<Integer, Integer> getMap()
    {
        return idToPort;
    }

}
