package com.producer.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.Stream;

/**
 * Produces data and sends it to the client.
 */
public class StreamProducer {

    private ServerSocket serverSocket;
    private PrintWriter outputStream;
    private Socket clientSocket;

    public StreamProducer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void sendData(Stream<String> data) throws IOException {
        try {
            clientSocket = serverSocket.accept();
            outputStream =
                    new PrintWriter(clientSocket.getOutputStream(), true);
            data.forEach(outputStream::println);
        } finally {
            close();
        }
    }

    private void close() throws IOException {
        outputStream.close();
        clientSocket.close();
    }
}
