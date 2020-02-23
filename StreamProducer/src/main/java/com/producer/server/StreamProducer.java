package com.producer.server;

import java.io.IOException;
import java.util.stream.Stream;

public interface StreamProducer {

    void sendData(Stream<String> data) throws IOException;
}
