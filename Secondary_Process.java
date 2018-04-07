/**
 *
 *class to handle all the secondary processes
 * Author: Ketan Joshi (ksj4205)
 */



import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;

import static java.lang.Thread.sleep;


public class Secondary_Process implements Runnable {


    int workID;// id == 1 is for sending thefinger table

    Socket socket;
    ObjectOutputStream out;
    ObjectInputStream in;

    public Secondary_Process(Socket socket)
    {
        this.socket = socket;

    }
    @Override
    public void run() {
    findthePricess();



    }

    private void findthePricess() {
        try {

            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());

         String s = in.readUTF();

        if (s.equalsIgnoreCase("rt"))
            SendfingerTable();

        if (s.equalsIgnoreCase("sf"))
            AddTheFile();

        if (s.equalsIgnoreCase("dl"))
            NodeDeletion();

        if (s.equalsIgnoreCase("gf"))
           RetrieveFile();
        
        if (s.equalsIgnoreCase("lb"))
            LoadBalance();
            


        }
            catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void LoadBalance() {
        try {
        String id = in.readUTF();
        int ID = Integer.parseInt(id);

        System.out.println("\nReceieved data for "+ID);

        HashSet<String> hs = null;

            hs = (HashSet<String>)in.readObject();
            Nodes.Directory.put(ID,hs);

            System.out.println(Nodes.Directory);
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    private void RetrieveFile() {
        try {

            String id = in.readUTF();
            int ID = Integer.parseInt(id);



            String filename = in.readUTF();
            System.out.println("\nRetrieve file request received for file"+ filename+" from Node "+ID);
          //  System.out.println(Nodes.Directory);

            //System.out.println(Nodes.Directory.get(ID).contains(filename));

            if(Nodes.Directory.get(ID).contains(filename))
            {
                out.writeUTF("s");
                out.flush();

            }
            else {
                out.writeUTF("f");
                out.flush();
            }
            System.out.println("File sent Successfully!\n");

            sleep(5000);
            out.close();
            in.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void NodeDeletion() {

        try {
            String id = in.readUTF();
            int ID = Integer.parseInt(id);

            String cs = in.readUTF();
            int count = Integer.parseInt(cs);
            System.out.println("\nNode "+ID+" is going offline!!");

            for(int i =0;i<count;i++)
            {
                String ct = in.readUTF();
                int iid = Integer.parseInt(ct);
                System.out.println("Receieved data for "+iid);

                HashSet<String> hs = (HashSet<String>)in.readObject();
                Nodes.Directory.put(iid,hs);
            }

            System.out.println("File added to the directory\n Current Directory:");
            System.out.println(Nodes.Directory+"\n");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private void AddTheFile() {
        System.out.println("\nReceieving a file!");
        try {

            String id = in.readUTF();
            int ID = Integer.parseInt(id);

            String filename = in.readUTF();

            if(!Nodes.Directory.containsKey(ID))
                Nodes.Directory.put(ID,new HashSet<>());
            Nodes.Directory.get(ID).add(filename);

            System.out.println(filename+" added added for node " +ID+"\n");
            //System.out.println(Nodes.Directory.get(ID));

            out.close();
            in.close();
            socket.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void SendfingerTable() {
       // System.out.println("Sending the fingertable");
        try {

            //ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(Nodes.fingerTable);
            out.flush();
            sleep(5000);
            out.close();
            in.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
