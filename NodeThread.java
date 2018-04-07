/**
 *
 *Node thread class to work with idividual node
 * Author: Ketan Joshi (ksj4205)
 */


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;

public class NodeThread implements Runnable{

    Socket node;
    ObjectOutputStream out;
    ObjectInputStream in;
    ObjectOutputStream out2;
    ObjectInputStream in2;

    int id;
    Socket CommunicateWithNode;

    public NodeThread (Socket node)
    {
        this.node = node;
        try {
            this.out = new ObjectOutputStream(node.getOutputStream());
            this.in = new ObjectInputStream(node.getInputStream());
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
    }


    @Override
    public void run() {
        //System.out.println("inside run");
        getCredentials();
    }

    public void getCredentials()
    {
       // System.out.println("inside get credentials");
        InetAddress ip = node.getInetAddress();
        try {
            //Initializing
            this.id = Integer.parseInt(in.readUTF());
            System.out.println("\nNode "+id+" connected\n");
            Registration_server.AliveNodes.put(id,true);
            Registration_server.Nodes.put(id,this);
            Registration_server.Sockets.put(id,this.node);
            Registration_server.PrintHashmap();

            CommunicateWithNode = new Socket(ip,8000+id);
            out2 = new ObjectOutputStream(CommunicateWithNode.getOutputStream());
            in2 = new ObjectInputStream(CommunicateWithNode.getInputStream());

            Registration_server.BroadcastAliveNodes();

            Processes();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void Processes() {

        Scanner sc = new Scanner(System.in);
        try{
            while (true)
            {
                String input = in.readUTF();
                switch (input)
                {
                    case "d":
                        System.out.println("\nNode "+id+" Going offline\n");
                        Registration_server.AliveNodes.remove(id);
                        Registration_server.BroadcastAliveNodes();
                        break;

                }



            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void SendAliveNodes(int[]alivenodes,InetAddress[]ips) {

        try {
            out2.writeUTF("u");
            out2.flush();
            out2.writeObject(alivenodes);
            out2.flush();
            out2.writeObject(ips);
            out2.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
