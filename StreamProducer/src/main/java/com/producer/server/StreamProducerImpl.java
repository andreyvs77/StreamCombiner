package com.producer.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.Stream;

public class StreamProducerImpl implements StreamProducer {

    private ServerSocket serverSocket;
    private PrintWriter outputStream;
    private Socket clientSocket;

    public StreamProducerImpl(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void sendData(Stream<String> data) throws IOException {
        clientSocket = serverSocket.accept();
        outputStream = new PrintWriter(clientSocket.getOutputStream(), true);
        data.forEach(outputStream::println);
        close();
    }

    private void close() throws IOException {
        outputStream.close();
        clientSocket.close();
    }
}
