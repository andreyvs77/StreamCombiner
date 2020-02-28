package com.client.combiner;

import javax.xml.bind.JAXBException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Serves to receive messages from the Producer and send them to the StreamCombiner fro processing.
 */
public class StreamReceiver implements Runnable {

    private static final Logger logger =
            Logger.getLogger(StreamReceiver.class.getName());

    private StreamCombiner streamCombiner;
    private Socket clientSocket;
    private BufferedReader inputStream;
    private String name;

    public StreamReceiver(StreamCombiner streamCombiner, String host,
                          int port) throws IOException {
        this.streamCombiner = streamCombiner;
        clientSocket = new Socket(host, port);
        name = host + port;
        streamCombiner.addNewStream(name);
        logger.info("client socket connected to " + host + ":" + port);
        inputStream = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()));
    }

    /**
     * Receives data from StreamProducer and send it for processing to the  StreamCombiner.
     *
     * @throws IOException
     */
    public void receiveData() throws IOException {
        inputStream.lines().forEach(data -> {
            try {
                streamCombiner.process(data, name);
            } catch (JAXBException e) {
                logger.log(Level.WARNING, "cannot process data - " +
                        data, e);
            }
        });
        streamCombiner.closeStream(name);
        clientSocket.close();
    }

    @Override
    public void run() {
        try {
            receiveData();
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }
    }
}
