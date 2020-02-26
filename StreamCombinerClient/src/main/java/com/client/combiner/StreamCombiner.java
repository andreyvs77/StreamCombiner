package com.client.combiner;

import javax.xml.bind.JAXBException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

public interface StreamCombiner<T> {

    default void process(String data, String streamReceiverName)
            throws JAXBException {
        addData(data, streamReceiverName);
        outputData();
    }

    T addData(String data, String streamReceiverName) throws JAXBException;

    Set<T> outputData();

    int addNewStream(String streamName);

    int removeStream(String name);

    Map<Long, BigDecimal> getUnsentData();
}
