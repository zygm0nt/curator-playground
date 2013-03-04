package pl.touk.curator;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author mcl
 */
public abstract class BaseExperimentRunner implements Closeable {

    private static Logger log = Logger.getLogger(BaseExperimentRunner.class);

    protected abstract String getPath();

    protected abstract BaseExperimentRunner instantiate(String zkAddr, String serverId, String path) throws Exception;

    protected abstract void process() throws Exception;

    public void run() throws Exception {
        List<Thread> threads = new ArrayList();

        final String path = getPath() + UUID.randomUUID().toString().split("-")[0];
        for (int i = 0; i < 3; i++) {
            final String serverId = "" + i;
            threads.add(new Thread() {
                @Override
                public void run() {
                    setName("Zk thread " + serverId);
                    try {
                        instantiate("localhost:2187", serverId, path).process();
                    } catch (Exception e) {
                        log.error("Thread error: ", e);
                    }
                }
            });
            threads.get(i).start();
        }

        Thread.sleep(50000);
    }

    public void close() throws IOException { }
}
