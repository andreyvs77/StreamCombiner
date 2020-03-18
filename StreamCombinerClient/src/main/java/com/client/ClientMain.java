package com.client;

import com.client.combiner.StreamCombiner;
import com.client.combiner.StreamReceiver;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Launches StreamReceivers to receive messages from Producers.
 */
public class ClientMain {

    private static final Logger logger =
            Logger.getLogger(ClientMain.class.getName());
    private static final String serverPropsFilename =
            File.separator + "servers.txt";
    private static Properties serverProps = new Properties();

    public static void main(String[] args)
            throws IOException, InterruptedException, JAXBException {
        var streamCombiner = new StreamCombiner();
        var executor =
                (ThreadPoolExecutor) Executors.newCachedThreadPool();
        loadProperties(args);
        //create clients for the servers
        for (Map.Entry<Object, Object> entry : serverProps.entrySet()) {
            Object portKey = entry.getKey();
            Object host = entry.getValue();
            int port = Integer.parseInt((String) portKey);
            executor.execute(
                    new StreamReceiver(streamCombiner, (String) host, port));

        }
        streamCombiner.shutdown();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Reads file with properties of servers.
     *
     * @param args Command line args.
     */
    private static void loadProperties(String[] args) throws IOException {
        InputStream input = null;
        try {
            if (args != null && args.length > 0) {
                input = new FileInputStream(
                        args[0] + serverPropsFilename);
            } else {
                input = ClientMain.class
                        .getResourceAsStream(serverPropsFilename);
            }
            serverProps.load(input);
        } catch (IOException e) {
            logger.warning(e.getMessage());
        } finally {
            if (input != null) {
                input.close();
            }
        }
    }
}
