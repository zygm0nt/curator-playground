package pl.touk.zookeeper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * @author mcl
 */
public abstract class ZkRunnerBase {

    protected static void prepareFiles(int numOfServers) {
        for (int i = 1; i <= numOfServers; i++) {
            File dir = new File("/tmp/zookeeper_" + i);
            if (dir.exists())
                dir.delete();
            dir.mkdir();
            File myid = new File("/tmp/zookeeper_" + i + "/myid");
            writeToFile(myid, "" + i);
        }
    }

    protected static void writeToFile(File f, String content) {
        try{
            FileWriter fstream = new FileWriter(f);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(content);
            out.close();
        }catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
