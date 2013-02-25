package pl.touk.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author mcl
 */
public class ZkCli {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        org.apache.zookeeper.ZooKeeperMain.main(new String[] {"-server", "localhost:2187"});

    }
}
