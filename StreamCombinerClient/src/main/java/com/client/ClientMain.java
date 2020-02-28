package com.client;

import com.client.combiner.StreamCombiner;
import com.client.combiner.StreamReceiver;

import javax.xml.bind.JAXBException;
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

    public static void main(String[] args)
            throws IOException, InterruptedException, JAXBException {
        var streamCombiner = new StreamCombiner();
        var executor =
                (ThreadPoolExecutor) Executors.newCachedThreadPool();
        //read file with properties of servers
        var serverPropsFilename = "servers.txt";
        var serverProps = new Properties();
        try (InputStream input = new FileInputStream(
                args[0] + serverPropsFilename)) {
            serverProps.load(input);
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }

        //create clients for the servers
        for (Map.Entry<Object, Object> entry : serverProps.entrySet()) {
            Object portKey = entry.getKey();
            Object host = entry.getValue();
            int port = Integer.parseInt((String) portKey);
            executor.execute(
                    new StreamReceiver(streamCombiner, (String) host, port));

        }
        logger.warning("AFTER EXECUTOR");
        streamCombiner.shutdown();
        logger.warning("AFTER streamCombiner.shutdown()");
        executor.shutdown();
        logger.warning("AFTER executor.shutdown()");
        executor.awaitTermination(1, TimeUnit.MINUTES);
        logger.warning("AFTER executor.awaitTermination");
    }
}
