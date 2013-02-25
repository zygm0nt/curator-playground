package pl.touk.zookeeper;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * @author mcl
 */
public class ZookeeperRunner extends ZkRunnerBase {
    
    public static void main(String[] args) throws Exception {
        prepareFiles(1);
        QuorumPeerMain quorumPeerMain = new QuorumPeerMain();
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parse(new Object().getClass().getResource("/zoo.cfg").getPath());
        quorumPeerMain.runFromConfig(config);
    }
}
