/**
 * The node in a distribution hash table
 *
 * Author: Ketan Joshi (ksj4205)
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.sleep;
import static java.util.Objects.hash;


public class Nodes implements Runnable {

    static int size = 16;
    static FingerTable fingerTable = new FingerTable();
    static int ID;
    ObjectOutputStream out ;
    ObjectInputStream in ;
    ObjectOutputStream out2;
    ObjectInputStream in2;
    static ServerSocket server;
    static ServerSocket server2;
    static ConcurrentHashMap<Integer,HashSet<String>> Directory = new ConcurrentHashMap<>();
    static HashSet<Integer> currentalivenodes = new HashSet<>();
    boolean Notfirsttime = false;

    int workID;

    public  Nodes(int workID)
    {
        this.workID = workID;
        Notfirsttime = false;
    }


    public static void main(String[] args)
    {
        Nodes node = new Nodes(1);
        node.StartProcess();
    }

    private void StartProcess() {
        Scanner sc = new Scanner(System.in);

        System.out.println("Enter he GUID:");
        ID = sc.nextInt();

        System.out.println("Enter the IP address");
        String ip = sc.next();

        Directory.put(ID,new HashSet<>());// creating a new hshset
        try {
            Nodes.server = new ServerSocket(8000+ID);
            Nodes.server2 = new ServerSocket(9000+ID);

            new Thread(new Nodes(2)).start();
            new Thread(new Nodes(3)).start();


            Socket s = new Socket(ip,6000);
            System.out.println("**Connection Established with Server!!\n");

            out = new ObjectOutputStream(s.getOutputStream());
             in = new ObjectInputStream(s.getInputStream());

            out.writeUTF(""+ID);
            out.flush();

            Menu();



        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void Menu()
    {
        Scanner sc = new Scanner(System.in);
        while (true){
            System.out.println("1.Print the Table    (p) \n" +
                    "2.Add a file         (a)\n" +
                    "3.Delete the node    (d)\n" +
            "4.Retrieve a file    (r)\n"+
            "5.View the Directory (v)");

            String input = sc.next();

            switch (input)
            {
                case "1":
                case "p":
                case "P":
                    synchronized (fingerTable) {
                        System.out.println(fingerTable);
                    }
                break;

                case "2":
                case "a":
                case "A":
                    AddNewFile();
                    break;

                case "3":
                case "d":
                case "D":

                DeleteTheNode();

                break;

                case "4":
                case "r":
                case "R":
                    RetrieveFile();

                    break;
                case "5":
                case "v":
                case "V":
                    System.out.println("The current files in the directory are\n"+Directory);
            }
        }
    }

    private void RetrieveFile() {


        Scanner sc = new Scanner(System.in);
        System.out.println("Enter the name of the file");
        String filename = sc.next();
        int destination = Math.abs(filename.hashCode() % size);
        System.out.println("Destination hash value: '"+destination+"'");

    /// Code to send find the target node.
        if (destination == ID)
        {
            System.out.println("The key is the Current node, File entered!");
          //  System.out.println(Directory.get(ID).add(filename));
        }
        if (Directory.containsKey(destination))
        {
            System.out.println("Current node contains files of the Node"+destination+"\n "+
            filename+" \n");
           // System.out.println(Directory.get(destination).add(filename));
        }
        // algorithm for sending the file

        RetrieveFileHelper(destination,filename);

    }
     private void RetrieveFileHelper(int destination,String filename)
     {
         int ogdestination =destination;
         try {

             FingerTable dummy = fingerTable;
             int successor = ID;
             int inode= ID;
             InetAddress ip = InetAddress.getByName("127.0.0.1");
             int diff = 9999;
             while (true) {
                // System.out.println("inside while");
                 for (int i = 0; i < 4; i++) {
                     int tdiff =0;
                  //   System.out.println("checkin for the node"+dummy.Table[i][0]);

                     if (dummy.Table[i][0] == destination) {

                         successor = dummy.Table[i][1];
                        System.out.println("Retrieving the file  node '"+destination+"' at node '"+successor+
                         "'");
                         ip = dummy.ips[i];

                        RetrieveActualFile(filename,successor,ip,ogdestination);
                        return;
                     }

                     if(ID > destination)
                     {
                         if(dummy.Table[i][0]<16 && dummy.Table[i][0] > ID)
                             tdiff = destination - dummy.Table[i][0] + 16;
                         else
                             tdiff = destination - dummy.Table[i][0];

                     }
                     else
                         tdiff = destination - dummy.Table[i][0];
                     if(tdiff>0 && diff > tdiff)
                     {
                       //  System.out.println("*"+dummy.Table[i][0]);
                         diff = tdiff;
                         successor = dummy.Table[i][1];
                         ip = dummy.ips[i];
                         inode = dummy.Table[i][0];


                     }

                 }
                 //System.out.println("asking node "+successor+" for fingertable");
                 if (Cycle(ID,inode,successor,destination))
                     destination = successor;

                 if (successor == destination)
                 {

                    // System.out.println("Retrieving the file  node"+destination+"at node "+successor);

                     RetrieveActualFile(filename,successor,ip,ogdestination);


                     // s.close();
                     return;

                 }

                //finding the file at some other node
                 Socket s2 = new Socket(ip, 9000 + successor);
                 ObjectOutputStream objout2 = new ObjectOutputStream(s2.getOutputStream());
                 ObjectInputStream objin2 = new ObjectInputStream(s2.getInputStream());

                 objout2.writeUTF("rt"); // rt for reading table
                 objout2.flush();


                 dummy = (FingerTable)objin2.readObject();
                // System.out.println("recived the dummy table");

                 sleep(5000);
                 objout2.close();
                 objin2.close();
                 s2.close();
                 diff = 999;
             }

         }
         catch (IOException e) {
             e.printStackTrace();
         } catch (ClassNotFoundException e) {
             e.printStackTrace();
         } catch (InterruptedException e) {
             e.printStackTrace();
         }

     }

    private void RetrieveActualFile(String filename, int successor, InetAddress ip, int destination) {
        try {
            Socket s = new Socket(ip, 9000 + successor);
            ObjectOutputStream objout = new ObjectOutputStream(s.getOutputStream());
            ObjectInputStream objin = new ObjectInputStream(s.getInputStream());

            objout.writeUTF("gf"); // getting the file
            objout.flush();

            objout.writeUTF(""+destination);
            objout.flush();

            objout.writeUTF(filename);
            objout.flush();

            String file = objin.readUTF();


            if(file.equalsIgnoreCase("s"))
            System.out.println(filename+"File receieved successfully!");

            else
                System.out.println(filename+" Not Found");

            sleep(6000);
            objout.close();
            s.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void DeleteTheNode() {

        try {
            out.writeUTF("d");
            out.flush();
            // sending all the data to the node
            int successor = 0;
            InetAddress ip = InetAddress.getByName("127.0.0.1");
            synchronized (fingerTable) {
                successor = fingerTable.Table[0][1];
              ip = fingerTable.ips[0];
            }
            Socket socket =new Socket(ip ,9000+successor);
            ObjectOutputStream objout = new ObjectOutputStream(socket.getOutputStream());

            objout.writeUTF("dl");
            objout.flush();
            objout.writeUTF(""+ID);
            objout.flush();
            int count = Directory.keySet().size();

            objout.writeUTF(""+count);
            objout.flush();

            for(int nid : Directory.keySet())
            {
                objout.writeUTF(""+nid);
                objout.flush();

                objout.writeObject(Directory.get(nid));
                objout.flush();

                Directory.remove(nid);

            }


            System.out.println("Going Offline!!!!");
            System.out.println("Sending all Data to Node '"+successor+"'");
            Directory.remove(ID);
            sleep(9000);
            objout.close();
            socket.close();
            System.exit(9);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void AddNewFile() {

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter the name of the file");
        String filename = sc.next();
        int destination = Math.abs(filename.hashCode() % size);
        System.out.println("The hash key of file is: "+destination);
        if (destination == ID)
        {
            System.out.println("The Destiation ID is the GUID of the current node"+
            "\n Saving the file in the Directory");
            Directory.get(ID).add(filename);
        }
        if (Directory.containsKey(destination))
        {
            System.out.println("The current node contains files of  Destiation ID"+
                    "\n Saving the file in the Directory");
            Directory.get(destination).add(filename);
        }
        // algorithm for sending the file

        else
            SendtheFileToCorrectNode(destination,filename);

    }

    private void SendtheFileToCorrectNode(int destination, String filename) {
        int ogdestination = destination;
       try {

            FingerTable dummy = fingerTable;
           int successor = ID;
           int inode= ID;
           InetAddress ip = InetAddress.getByName("127.0.0.1");
           int diff = 9999;
           while (true) {
               //System.out.println("inside while");
               for (int i = 0; i < 4; i++) {
                   int tdiff =0;
                  // System.out.println("checkin for the node"+dummy.Table[i][0]);

                   if (dummy.Table[i][0] == destination) {

                       successor = dummy.Table[i][1];
                       System.out.println("sending the file to node '"+destination+"' at node '"+successor+"'");
                       ip = dummy.ips[i];
                       Socket s = new Socket(ip, 9000 + successor);
                       ObjectOutputStream objout = new ObjectOutputStream(s.getOutputStream());
                       objout.writeUTF("sf"); // s for sending file
                       objout.flush();

                       objout.writeUTF(""+ogdestination);
                       objout.flush();


                       objout.writeUTF(filename);
                       objout.flush();
                       sleep(5000);
                        s.close();
                       return;

                   }

                    if(ID > destination)
                    {
                        if(dummy.Table[i][0]<16 && dummy.Table[i][0] > ID)
                            tdiff = destination - dummy.Table[i][0] + 16;
                        else
                        tdiff = destination - dummy.Table[i][0];

                    }
                    else
                         tdiff = destination - dummy.Table[i][0];
                   if(tdiff>0 && diff > tdiff)
                   {
                       //System.out.println("*"+dummy.Table[i][0]);
                       diff = tdiff;
                       successor = dummy.Table[i][1];
                       ip = dummy.ips[i];
                       inode = dummy.Table[i][0];


                   }

               }
               //System.out.println("asking node "+successor+" for fingertable");
             //  if(inode<destination && successor > destination ||
               //        (inode > destination && successor >destination && successor < ID))
               if (Cycle(ID,inode,successor,destination))
                   destination = successor;

               if (successor == destination)
               {

                   System.out.println("sending the file to node '"+destination+"' at node '"+successor+"'");

                   Socket s = new Socket(ip, 9000 + successor);
                   ObjectOutputStream objout = new ObjectOutputStream(s.getOutputStream());
                   objout.writeUTF("sf"); // s for sending file
                   objout.flush();

                   objout.writeUTF(""+ogdestination);
                   objout.flush();

                   objout.writeUTF(filename);
                   objout.flush();

                   sleep(5000);
                   objout.close();
                  // s.close();
                   return;

               }

               Socket s2 = new Socket(ip, 9000 + successor);
               ObjectOutputStream objout2 = new ObjectOutputStream(s2.getOutputStream());
               ObjectInputStream objin2 = new ObjectInputStream(s2.getInputStream());

               objout2.writeUTF("rt"); // rt for reading table
               objout2.flush();


               dummy = (FingerTable)objin2.readObject();
              // System.out.println("recived the dummy table");

               sleep(5000);
               objout2.close();
               objin2.close();
               s2.close();
               diff = 999;
           }

       }
        catch (IOException e) {
           e.printStackTrace();
       } catch (ClassNotFoundException e) {
           e.printStackTrace();
       } catch (InterruptedException e) {
           e.printStackTrace();
       }
    }

    private boolean Cycle(int id, int inode, int successor, int destination) {

        if (successor> inode && successor > destination)
         if (destination > inode ) {
             //System.out.println("*****C1");
             return true;
         }

        if(inode > destination && inode> successor && inode > id)
        {
            if (inode < (destination + 16) && (destination +16) < (successor+16))
            {
                //System.out.println("*****C2");
                return true;
            }
        }

        if (destination > inode && destination > successor)
        {
            if ((successor+16)> destination && (successor+16)>inode && (inode+16)>(successor+16)) {
             //   System.out.println("*****C3");
                return true;
            }
        }

        return false;
    }


    private void FindtheProcess() {

        try {

        Socket regist_server = server.accept();
        out2 = new ObjectOutputStream(regist_server.getOutputStream());
        in2 = new ObjectInputStream(regist_server.getInputStream());


        while (true) {

            String input = in2.readUTF();

            switch (input) {
                case "u":
                    CalculateFingerTable();
                    break;
            }
        }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void  CalculateFingerTable()
    {

     try {
         int[] alivenodes = (int[]) in2.readObject();
         InetAddress[] ips = (InetAddress[])in2.readObject();
        for(int i = 0;i<4;i++)
        {
            int entry= (ID + (int)(Math.pow(2,i)))%size;
            fingerTable.Table[i][0]= entry;
            int successor=99999;
            int min = 999999;
            InetAddress minIp=InetAddress.getByName("127.0.0.1");
            InetAddress ip = InetAddress.getByName("127.0.0.1");;

            for(int j =0;j<alivenodes.length;j++)
            {
                int e1 = alivenodes[j]; // entry in the table
                if(e1>entry && e1<successor) {
                    successor = e1;
                    ip = ips[j];
                }

                if(entry == e1) {
                    successor = e1;
                    ip = ips[j];
                    break;
                }
                if(min>e1)
                {
                    min = e1;
                    minIp = ips[j];
                }
            }
            if(successor == 99999) {
                successor = min;
                ip = minIp;
            }
            fingerTable.Table[i][1]= successor;
            fingerTable.ips[i] = ip;


        }
         if(Notfirsttime && Directory.size()>1)
             LoadBalancer(alivenodes,ips);

         Notfirsttime = true;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void LoadBalancer(int[] alivenodes, InetAddress[] ips) {
        ArrayList tp= new ArrayList<>(Arrays.asList(alivenodes));
        HashSet<Integer> temp = new HashSet<>();
        HashSet <Integer>t2 = new HashSet<>();
        for(int j: alivenodes)
        {
            temp.add(j);
            t2.add(j);
        }


        temp.removeAll(currentalivenodes);
       // System.out.println("the subtracted set:"+temp);
        ArrayList<Integer> sentNode = new ArrayList<>();

        for(int values : Directory.keySet())
        {
            int dist = Math.min(Math.abs(values-ID),Math.abs(ID-values+16));
            int min = ID;
            for(int keys : temp)
            {
                if(keys!=ID && values !=ID) {
                    if (Cycle2(min, values, keys)) {
                        min = keys;

                    }
                }

            }

            if(min!=ID)
            {
                sentNode.add(values);
                for(int i =0;i<alivenodes.length;i++)
                {
                   if(min == alivenodes[i])
                   {
                       try {
                           Socket s = new Socket(ips[i],9000+min);
                           ObjectOutputStream objout = new ObjectOutputStream(s.getOutputStream());

                           objout.writeUTF("lb"); // load balancing
                           objout.flush();

                           objout.writeUTF(""+values);
                           objout.flush();

                           objout.writeObject(Directory.get(values));
                           objout.flush();

                           sleep(2000);
                           objout.close();
                           s.close();


                       } catch (IOException e) {
                           e.printStackTrace();
                       } catch (InterruptedException e) {
                           e.printStackTrace();
                       }
                   }

                }

                for(int val : sentNode)
                {
                    Directory.remove(val);
                }
                //code to send the data to that id
            }


        }

        currentalivenodes = t2;
       // System.out.println(Directory);
    }

    private boolean Cycle2(int id, int values, int keys) {
        //id is the id of the node which has the values
        //keys is the new node emerged
        //value is the value of the node ahead
        //find the cycle in clockwise values- keys - id
        if(keys == values)
            return true;
        if(values<keys && keys < id)
        {
            //123
            //System.out.println("cond1");
            return true;
        }
        if(values < keys  && values > id)
        {
            //14 15 2
            //System.out.println("cond2");
            if(id< keys && keys < id + 16 )
                return true;
        }

        if(values>keys && values >id) {
            //14 2 3
           // System.out.println("cond3");
            if (values < keys + 16 && keys + 16 < id + 16)
                return true;
        }
        return false;
    }

    @Override
    public void run() {
       if (this.workID == 2)
        FindtheProcess();

       if(this.workID == 3)
           ListeningProcess();

    }

    private void ListeningProcess() {

        // this process continuosly keep on listening
        //System.out.print("inside listening process");
        while (true)
        {
            try {
                Socket temp = server2.accept();
                //System.out.println("connection receieved for server 2");
                new Thread(new Secondary_Process(temp)).start();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


}
