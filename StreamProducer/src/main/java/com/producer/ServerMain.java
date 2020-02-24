package com.producer;

import com.producer.server.StreamProducerImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ServerMain {

    private static final Logger logger =
            Logger.getLogger(ServerMain.class.getName());

    public static void main(String[] args)
            throws IOException, InterruptedException {
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

        //run all servers
        for (Map.Entry<Object, Object> entry : serverProps.entrySet()) {
            int port = Integer.parseInt((String)entry.getKey());
            String fileName = (String) entry.getValue();
            var streamProducer = new StreamProducerImpl(port);
            logger.info("Stream producer started on port " + port);
            var stream = Files.lines(Paths.get(args[0] + fileName));

            executor.execute(() -> {
                try {
                    streamProducer.sendData(stream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }
}
