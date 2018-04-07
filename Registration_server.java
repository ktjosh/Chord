/**
 *
 * The file contains the Registration Server
 * Author: Ketan Joshi (ksj4205)
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.hash;


public class Registration_server {

    static ConcurrentHashMap<Integer,Boolean>  AliveNodes = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,NodeThread> Nodes = new ConcurrentHashMap<>();
    static ConcurrentHashMap<Integer,Socket>Sockets = new ConcurrentHashMap<>();


    public static void main(String[] args)
    {
        //server socket
        try {
            ServerSocket server = new ServerSocket(6000);
            while (true) {
                Socket temp = server.accept();
              //  System.out.println("1 connection established!!");
                new Thread(new NodeThread(temp)).start();
                BroadcastAliveNodes();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void PrintHashmap()
    {

        System.out.println("Current Alive Nodes:\n"+AliveNodes);

    }

    public static void BroadcastAliveNodes()
    {
        int i =0;
        int[] alivenodes= new int[AliveNodes.keySet().size()];
        InetAddress[] ips = new InetAddress[AliveNodes.keySet().size()];
        for (int nodes:AliveNodes.keySet()) {
            alivenodes[i]=  nodes;
            ips[i] =Sockets.get(nodes).getInetAddress();
            //System.out.println("+"+ips[i]);
            i++;

        }
        for (int nodes : AliveNodes.keySet())
        {
            NodeThread temp = Nodes.get(nodes);
            temp.SendAliveNodes(alivenodes,ips);


        }
    }
}
