package com.github.scaars10.pecanraft.dirTest;

import com.github.scaars10.pecanraft.server.PecanConfig;
import com.github.scaars10.pecanraft.server.PecanServer;

import java.util.Scanner;

public class StartServers
{
    PecanServer [] servers;
    //Thread []serverThreads;
    public static void main(String[] args)
    {
        StartServers ob = new StartServers();
        int num = 3;

        PecanConfig config = new PecanConfig();
        ob.servers = new PecanServer[num];
        for(int i=0;i<num;i++)
        {
            ob.servers[i] = new PecanServer(i, config);
        }
        System.out.println("Starting Servers...");
        for(int i=0;i<num;i++) {
            int finalI = i;
            new Thread(()->ob.servers[finalI].start()).start();

        }
        Scanner in = new Scanner(System.in);
        label:
        while(true)
        {
            String input = in.nextLine();
            String command = input.split(" ")[0];

            int id = Integer.parseInt(input.split(" ")[1]);
            if(id>=num)
                continue;
            switch (command) {
                case "start":
                    if (ob.servers[id]!=null && ob.servers[id].isRunning()) {
                        System.out.println("Server " + id + " is already running..");
                    } else {
                        System.out.println("server server "+id);
                        new Thread(() ->
                        {
                            ob.servers[id] = new PecanServer(id, config);
                            ob.servers[id].start();
                        }).start();

                    }
                    break;
                case "stop":

                    if (ob.servers[id]!=null && ob.servers[id].isRunning())
                    {
                        System.out.println("Stopping server "+id);
                        ob.servers[id].stop();
                        System.out.println(ob.servers[id].isRunning());
                        ob.servers[id] = null;
                    }
                    else {
                        System.out.println("Server " + id + " is not running..");
                    }
                    break;
                case "exit":
                    break label;
                default:
                    System.out.println("Enter valid command...");
                    break;
            }

        }
    }
}
