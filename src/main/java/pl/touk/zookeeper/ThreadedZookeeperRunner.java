package pl.touk.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mcl
 */
public class ThreadedZookeeperRunner extends ZkRunnerBase {
    
    private static Logger log = Logger.getLogger(ThreadedZookeeperRunner.class);

    private static final int NUM_OF_THREADS = 3;

    public static void main(String[] args) throws Exception {
        new ThreadedZookeeperRunner().run();
    }

    private void run() throws Exception {
        prepareFiles(NUM_OF_THREADS);
        System.setProperty("zookeeper.jmx.log4j.disable", "true");

        List<Thread> threads = new ArrayList();
        for (int i = 0; i < NUM_OF_THREADS; i++){
            final int threadNum = i + 1;
            final String path = getClass().getResource("/zoo_" +  threadNum + ".cfg").getPath();
            threads.add(new Thread() {
                @Override
                public void run() {
                    setName("Zk thread " + threadNum);
                    QuorumPeerMain quorumPeerMain = new QuorumPeerMain();
                    QuorumPeerConfig config = new QuorumPeerConfig();
                    try {
                        log.debug("Config path: " + path);
                        config.parse(path);
                        quorumPeerMain.runFromConfig(config);
                    } catch (Exception ex) {
                        log.error("Got exception: ", ex);
                    }
                    log.debug("leaving...");
                }
            });
            threads.get(threads.size()-1).start();
        }

        //new Exhibitor().start();

        Thread.sleep(5000);

        for (Thread thread : threads) thread.join();
    }


}
