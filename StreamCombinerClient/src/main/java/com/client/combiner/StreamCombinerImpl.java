package com.client.combiner;

import com.client.model.Data;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamCombinerImpl implements StreamCombiner<Data> {

    private static final Logger logger =
            Logger.getLogger(StreamCombinerImpl.class.getName());

    private JAXBContext jaxbContext;
    private ConcurrentSkipListMap<Long, BigDecimal> data;
    private final MaxStreamTimestamps maxStreamTimestamps;
    private CopyOnWriteArraySet<String> streamNames;
    private ConcurrentLinkedQueue<Data> output;
    private ConcurrentHashMap<String, Boolean> activityStatuses;
    private ConcurrentHashMap<String, Long> streamDataCounts;
    private final ReentrantLock lock = new ReentrantLock();

    public StreamCombinerImpl() throws JAXBException {
        jaxbContext = JAXBContext.newInstance(Data.class);
        data = new ConcurrentSkipListMap<>();
        maxStreamTimestamps = new MaxStreamTimestamps();
        streamNames = new CopyOnWriteArraySet<>();
        output = new ConcurrentLinkedQueue<>();
        activityStatuses = new ConcurrentHashMap<>();
        streamDataCounts = new ConcurrentHashMap<>();
    }

    @Override
    public Data addData(String input, String streamReceiverName)
            throws JAXBException {
        //unmarshal data
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        StringReader reader = new StringReader(input);
        Data inputData = (Data) unmarshaller.unmarshal(reader);
        //put data to sorted map
        Long timestamp = inputData.getTimestamp();
        BigDecimal amount = inputData.getAmount();
        streamNames.add(streamReceiverName);
        try {
            lock.lock();
            //TODO !!!!!!!!!!!!!!!!!! use merge !!!!!!!!!!!!!!!!!!
            data.computeIfPresent(timestamp, (key, value) -> value.add(amount));
            data.putIfAbsent(timestamp, amount);
            //put to maxStreamTimestamps last timestamp
            maxStreamTimestamps.put(streamReceiverName, timestamp);
            streamDataCounts.merge(streamReceiverName, 1L, Long::sum);
        } finally {
            lock.unlock();
        }

        //return data
        logger.fine(inputData.toString());
        return inputData;
    }

    @Override
    public Set<Data> outputData() {
        try {
            lock.lock();

            Set<Data> result = new LinkedHashSet<>();
            while (
                    (activityStatuses
                            .entrySet()
                            .stream()
                            .filter(
                                    Map.Entry::getValue).count() <=
                            maxStreamTimestamps.getStreamCount()
                    )
                            && data.size() > 0
                            && maxStreamTimestamps.getStreamNames()
                            .containsAll(activityStatuses.entrySet().stream()
                                    .filter(Map.Entry::getValue)
                                    .map(entry -> entry.getKey())
                                    .collect(
                                            Collectors.toSet()))
            ) {

//                Set<String> list =
//                        activityStatuses.entrySet().stream()
//                                .filter(Map.Entry::getValue)
//                                .map(entry->entry.getKey())
//                                .collect(
//                                        Collectors.toSet());
//                System.out.println(list);
//                System.out.println(maxStreamTimestamps.getStreamNames());
//
//                System.out.println(maxStreamTimestamps.getStreamNames().containsAll(list));

                logger.info(
                        "activityStatuses.get() - " +
                                activityStatuses.entrySet().stream().filter(
                                        Map.Entry::getValue).count() + " = " +
                                "streamMaxTimestamps.size() - " +
//                        streamMaxTimestamps.size()
                                maxStreamTimestamps.getStreamCount()
                );
                logger.info(maxStreamTimestamps.toString());


                //all streams sent data so we can send some data to output
                Long leastMaxTimestamp =
                        maxStreamTimestamps.pollMinTimestamp();
                logger.info("leastMaxTimestamp - " + leastMaxTimestamp);
                logger.info("before - " + data.size());
                data.forEach((k, v) -> logger.info(k + "=" + v));
                logger.info("==before");
                //prepare data for output
                Map<Long, BigDecimal> resultMap =
                        prepareData(data, leastMaxTimestamp);
                //convert to Set<Data>
                result.addAll(convertData(resultMap));
                logger.info("after");
                data.forEach((k, v) -> logger.info(k + "=" + v));
                logger.info("after==");
                if (result.size() > 0) {
                    sendDataToStdOut(result);
                }

                Set<String> list =
                        activityStatuses.entrySet().stream()
                                .filter(Map.Entry::getValue)
                                .map(entry -> entry.getKey())
                                .collect(
                                        Collectors.toSet());
                System.out.println("activityStatuses - " + list);
                System.out.println("maxStreamTimestamps - " +
                        maxStreamTimestamps.getStreamNames());

                System.out.println(
                        maxStreamTimestamps.getStreamNames().containsAll(list));
                System.out.println(data.size() > 0);
                System.out.println(activityStatuses
                        .entrySet()
                        .stream()
                        .filter(
                                Map.Entry::getValue).count() <=
                        maxStreamTimestamps.getStreamCount());
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    private void sendDataToStdOut(Set<Data> result) {
        logger.info(Thread.currentThread().getName() +
                " send data to standart output :");
        Data data = null;
        while ((data = output.poll()) != null)
            System.out.println(data);
    }

    private Set<Data> convertData(Map<Long, BigDecimal> input) {
        Set<Data> result = new LinkedHashSet<>();
        input.forEach(
                (timestamp, amount) -> {
                    result.add(new Data(timestamp, amount));
                    output.add(new Data(timestamp, amount));
                });
        return result;
    }

    /**
     * Clean data that we sand to the output from the ConcurrentSkipListMap<Long, BigDecimal> data and move them to the result LinkedHashMap.
     *
     * @param input        ConcurrentSkipListMap<Long, BigDecimal> Map-object that contains data for extraction.
     * @param maxTimestamp max Long value for key in input object to extraction.
     * @return LinkedHashMap that contains value for output.
     */
    private Map<Long, BigDecimal> prepareData(
            ConcurrentSkipListMap<Long, BigDecimal> input,
            Long maxTimestamp) {
        Map<Long, BigDecimal> result = new LinkedHashMap<>();
        input.entrySet().stream().
                takeWhile(n -> maxTimestamp.compareTo(n.getKey()) >= 0).
                forEach(e -> {
                    logger.info("---" + e);
                    result.put(e.getKey(), e.getValue());
                    input.remove(e.getKey());
                });
        return result;
    }

    @Override
    public int addNewStream(String streamName) {
        activityStatuses.put(streamName, true);
        return 0;
    }

    @Override
    public int removeStream(String name) {
        streamNames.remove(name);
        activityStatuses.put(name, false);
        return 0;
    }

    @Override
    public Map<Long, BigDecimal> getUnsentData() {
        return data;
    }

    private static class MaxStreamTimestamps {
        private HashMap<String, Long> streamTimestamps;
        private TreeSet<Long> timestamps;
        private final ReentrantLock timeLock = new ReentrantLock();

        public MaxStreamTimestamps() {
            streamTimestamps = new HashMap<>();
            timestamps = new TreeSet<>();
        }

        public void put(String streamName, Long timestamp) {
            try {
                System.out.println(Thread.currentThread().getName());
                timeLock.lock();
                Long timeValue = streamTimestamps.get(streamName);
                if (timeValue != null) {
                    timestamps.remove(timeValue);
                }
                streamTimestamps.put(streamName, timestamp);
                timestamps.add(timestamp);
            } finally {
                timeLock.unlock();
            }
        }

        public Long pollMinTimestamp() {
            try {
                timeLock.lock();
                Long result = timestamps.pollFirst();
                streamTimestamps.entrySet()
                        .removeIf(entry -> entry.getValue().equals(result));
                return result;
            } finally {
                timeLock.unlock();
            }
        }

        public int getStreamCount() {
            return streamTimestamps.size();
        }

        public Set<String> getStreamNames() {
            return streamTimestamps.keySet();
        }

        @Override
        public String toString() {
            return "MaxStreamTimestamps{" +
                    "streamTimestamps=" + streamTimestamps +
                    ", timestamps=" + timestamps +
                    '}';
        }
    }
}
