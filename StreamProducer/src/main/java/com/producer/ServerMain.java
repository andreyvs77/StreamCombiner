package com.producer;

import com.producer.server.StreamProducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Creates few StreamProducers using information from the files located in resource folder and send messages to the client using created StreamProducers.
 */
public class ServerMain {

    private static final Logger logger =
            Logger.getLogger(ServerMain.class.getName());
    private static final String serverPropsFilename =
            File.separator + "servers.txt";
    private static Properties serverProps = new Properties();

    public static void main(String[] args)
            throws IOException, InterruptedException {
        var executor =
                (ThreadPoolExecutor) Executors.newCachedThreadPool();
        loadProperties(args);
        //run all servers
        for (Map.Entry<Object, Object> entry : serverProps.entrySet()) {
            int port = Integer.parseInt((String) entry.getKey());
            String fileName = (String) entry.getValue();
            var streamProducer = new StreamProducer(port);
            logger.info("Stream producer started on port " + port);
            var stream = getStringStream(args, File.separator +
                    fileName);

            executor.execute(() -> {
                try {
                    streamProducer.sendData(stream);
                } catch (IOException e) {
                    logger.warning(e.getMessage());
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Get Stream of strings from a file with data.
     *
     * @param args     Command line args.
     * @param fileName File name of the file with data.
     * @return Stream of strings from a file.
     * @throws IOException
     */
    private static Stream<String> getStringStream(String[] args,
                                                  String fileName)
            throws IOException {
        Stream.Builder<String> builder
                = Stream.builder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(getInputStream(args, fileName)))) {
            String line;
            while ((line = br.readLine()) != null) {
                builder.add(line);
            }
        }
        return builder.build();
    }

    /**
     * Reads file with properties of servers.
     *
     * @param args Command line args.
     */
    private static void loadProperties(String[] args) throws IOException {
        InputStream input = null;
        try {
            input = getInputStream(args, serverPropsFilename);
            serverProps.load(input);
        } catch (IOException e) {
            logger.warning(e.getMessage());
        } finally {
            if (input != null) {
                input.close();
            }
        }
    }

    /**
     * Get InputStream from a file.
     *
     * @param args     Command line args.
     * @param fileName File name of the file with data.
     * @return InputStream from a file.
     * @throws FileNotFoundException
     */
    private static InputStream getInputStream(String[] args, String fileName)
            throws FileNotFoundException {
        InputStream input;
        if (args != null && args.length > 0) {
            input = new FileInputStream(
                    args[0] + fileName);
        } else {
            input = ServerMain.class
                    .getResourceAsStream(fileName);
        }
        return input;
    }
}
