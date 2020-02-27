package com.client.combiner;

import com.client.model.Data;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 */
public class StreamCombiner {

    private static final Logger logger =
            Logger.getLogger(StreamCombiner.class.getName());

    private JAXBContext jaxbContext;
    private ConcurrentSkipListMap<Long, BigDecimal> data;
    private final MaxStreamTimestamps maxStreamTimestamps;
    //    private CopyOnWriteArraySet<String> streamNames;
    private ConcurrentLinkedQueue<Data> output;
    private ConcurrentHashMap<String, Boolean> activityStatuses;
    //    private ConcurrentHashMap<String, Long> streamDataCounts;
    private final ReentrantLock lock;
    private int lastDataTimeout = 1000;
    private LinkedHashSet<Data> totalResult;
    private ExecutorService executorService;
    private Runnable outputRunner;
    private CountDownLatch outputLatch;
    private Jsonb jsonb;

    public StreamCombiner() throws JAXBException {
        jsonb = JsonbBuilder.create();
        lock = new ReentrantLock();
        outputRunner = new OutputRunner();
        outputLatch = new CountDownLatch(1);
        jaxbContext = JAXBContext.newInstance(Data.class);
        data = new ConcurrentSkipListMap<>();
        maxStreamTimestamps = new MaxStreamTimestamps();
//        streamNames = new CopyOnWriteArraySet<>();
        output = new ConcurrentLinkedQueue<>();
        activityStatuses = new ConcurrentHashMap<>();
//        streamDataCounts = new ConcurrentHashMap<>();
        totalResult = new LinkedHashSet<>();
    }

    /**
     * Process message from input stream.
     *
     * @param input              Input message.
     * @param streamReceiverName Name of the input stream.
     * @return Converted Data object that will be sent to the output.
     * @throws JAXBException
     */
    public Data process(String input, String streamReceiverName)
            throws JAXBException {
        //unmarshal data
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        StringReader reader = new StringReader(input);
        Data inputData = (Data) unmarshaller.unmarshal(reader);
        //put data to sorted map
        Long timestamp = inputData.getTimestamp();
        BigDecimal amount = inputData.getAmount();
//        streamNames.add(streamReceiverName);
        try {
            lock.lock();
            //TODO !!!!!!!!!!!!!!!!!! use merge !!!!!!!!!!!!!!!!!!
//            data.computeIfPresent(timestamp, (key, value) -> value.add(amount));
//            data.putIfAbsent(timestamp, amount);
            data.merge(timestamp, amount, BigDecimal::add);
            //put to maxStreamTimestamps last timestamp
            maxStreamTimestamps.put(streamReceiverName, timestamp);
//            streamDataCounts.merge(streamReceiverName, 1L, Long::sum);
            if (executorService == null) {
                executorService = Executors.newSingleThreadExecutor();
                executorService.submit(outputRunner);
            }
        } finally {
            lock.unlock();
        }

        //return data
        logger.info(inputData.toString());
        return inputData;
    }

    /**
     * Get all Data objects that have been sent to the output.
     *
     * @return Dispatched set of Data objects.
     */
    public LinkedHashSet<Data> getTotalResult() {
        return totalResult;
    }

    private void outputData() {
        try {
            lock.lock();
            if (maxStreamTimestamps.getStreamNames()
                    .containsAll(getActiveStreams())) {
                logger.info("available streams - " +
                        maxStreamTimestamps.toString());
                logger.info(
                        "active streams - " + getActiveStreams().toString());
                //all streams sent data so we can send some data to output
                Long leastMaxTimestamp =
                        maxStreamTimestamps.pollMinTimestamp();
                logger.info("leastMaxTimestamp - " + leastMaxTimestamp);
                logger.info("unsent data before processing - " + data.size());
                data.forEach((k, v) -> logger.info(k + "=" + v));
                //prepare data for output
                Map<Long, BigDecimal> resultMap =
                        prepareData(data, leastMaxTimestamp);
                //convert to Set<Data>
                totalResult.addAll(convertData(resultMap));
                logger.info("unsent data after processing - ");
                data.forEach((k, v) -> logger.info(k + "=" + v));
                if (totalResult.size() > 0) {
                    sendDataToStdOut();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Is used to close current object when it does not need to process any data.
     *
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        outputLatch.await();
    }

    /**
     * Output results to the standard output
     */
    private void sendDataToStdOut() {
        Data data;
        while ((data = output.poll()) != null) {
            System.out.println(jsonb.toJson(data));
        }
    }

    /**
     * Convert data from Map.Entry to the Data object.
     *
     * @param input Map that contains data for extraction.
     * @return Set of converted objects.
     */
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
     * @param input        ConcurrentSkipListMap<Long, BigDecimal> object that contains data for extraction.
     * @param maxTimestamp Max timestamp in input object to extraction.
     * @return LinkedHashMap object that contains value for output.
     */
    private Map<Long, BigDecimal> prepareData(
            ConcurrentSkipListMap<Long, BigDecimal> input,
            Long maxTimestamp) {
        Map<Long, BigDecimal> result = new LinkedHashMap<>();
        input.entrySet().stream().
                takeWhile(entry -> maxTimestamp.compareTo(entry.getKey()) >= 0).
                forEach(entry -> {
                    logger.info("entry for output" + entry);
                    result.put(entry.getKey(), entry.getValue());
                    input.remove(entry.getKey());
                });
        return result;
    }

    /**
     * Add new stream to the StreamCombiner. If do not add the stream using this method the data from such stream will not be processed.
     *
     * @param streamName Name of the new stream.
     */
    public void addNewStream(String streamName) {
        activityStatuses.put(streamName, true);
//        return 0;
    }

    /**
     * Close the stream (set boolean flag to false). If do not close the stream StreamCombiner will be waiting for the new data from this stream.
     *
     * @param name Name of the closed stream.
     */
    public void closeStream(String name) {
//        streamNames.remove(name);
        activityStatuses.put(name, false);
//        return 0;
    }

    /**
     * Get streams that did not close by the client or after timeout.
     *
     * @return Set of active streams.
     */
    private Set<String> getActiveStreams() {
        return activityStatuses.entrySet().stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Get unsent to output data.
     *
     * @return Map object that contains unsent data.
     */
    public Map<Long, BigDecimal> getUnsentData() {
        return data;
    }

    /**
     * Contains information about max timestamps in every stream.
     */
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
                timeLock.lock();
                Long timeValue = streamTimestamps.get(streamName);
                logger.info("old timeValue - " + timeValue);
                if (timeValue != null) {
                    long countTimeValue = streamTimestamps.values().stream()
                            .filter(v -> v.equals(timeValue)).count();
                    //if only one such timestamp in set than remove it to replace by new value
                    if (countTimeValue == 1) {
                        timestamps.remove(timeValue);
                    }
                }
                logger.info("new timestamp - " + timestamp);
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

    /**
     * Serves for output data in a loop while data containes data.
     */
    private class OutputRunner implements Runnable {

        @Override
        public void run() {
            loopOutput();
            logger.info("finish output; data size - " + data.size());
            logger.info("finish output" + getActiveStreams().size());
            if (lastDataTimeout > 0) {
                try {
                    Thread.sleep(lastDataTimeout);
                    loopOutput();
                } catch (InterruptedException e) {
                    logger.warning(e.getMessage());
                }
            }
            outputLatch.countDown();
        }

        private void loopOutput() {
            while (data.size() > 0 || getActiveStreams().size() > 0) {
                outputData();
            }
        }
    }

}
