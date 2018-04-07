/**
 *contains the finger table
 *
 * Author: Ketan Joshi (ksj4205)
 */
import java.io.Serializable;
import java.net.InetAddress;


public class FingerTable  implements Serializable{

    int[][] Table;
    InetAddress[] ips;

    public FingerTable()
    {

        Table = new int[4][2];
        ips = new InetAddress[4];

    }

    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("\nindex\t").append("entry\t").append("sucessor\t\n");
        for(int i =0;i<4;i++)
        {
            str.append(i+"\t\t").append(Table[i][0]+"\t\t").append(Table[i][1]+"\t")
            .append(ips[i]+"\n");
        }

        return str.toString();
    }
}
