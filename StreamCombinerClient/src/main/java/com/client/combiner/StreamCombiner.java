package com.client.combiner;

import com.client.model.Data;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Serves to process messages from Producers.
 */
public class StreamCombiner {

    private static final Logger logger =
            Logger.getLogger(StreamCombiner.class.getName());

    private JAXBContext jaxbContext;
    /**
     * Stores parsed input data.
     */
    private ConcurrentSkipListMap<Long, BigDecimal> data;
    /**
     * Contains information about max timestamps in every stream.
     */
    private final MaxStreamTimestamps maxStreamTimestamps;
    /**
     * Stores data that will be output in standard output.
     */
    private ConcurrentLinkedQueue<Data> output;
    /**
     * Contains information about statuses every registered stream (active/inactive).
     */
    private ConcurrentHashMap<String, Boolean> activityStatuses;
    private final ReentrantLock lock;
    /**
     * Max timeout after which the stream is considered as inactive.
     */
    private int timeout = 10 * 1000;
    /**
     * Time that need to wait all streams finish sending their data.
     */
    private int lastDataTimeout = 1000;
    /**
     * Contains all data that is processed by this StreamCombiner.
     */
    private LinkedHashSet<Data> totalResult;
    private ExecutorService executorService;
    /**
     * Serves for output data in a loop while data field contains entities.
     */
    private Thread outputRunner;
    private CountDownLatch outputLatch;
    private Jsonb jsonb;
    /**
     * Checks if streams hang. If it is true StreamActivityChecker sets such stream as inactive after timeout.
     */
    private StreamActivityChecker streamActivityChecker;

    public StreamCombiner() throws JAXBException {
        executorService = Executors.newFixedThreadPool(2);
        jsonb = JsonbBuilder.create();
        lock = new ReentrantLock();
        outputRunner = new Thread(new OutputRunner());
        jaxbContext = JAXBContext.newInstance(Data.class);
        data = new ConcurrentSkipListMap<>();
        maxStreamTimestamps = new MaxStreamTimestamps();
        output = new ConcurrentLinkedQueue<>();
        activityStatuses = new ConcurrentHashMap<>();
        totalResult = new LinkedHashSet<>();
        outputLatch = new CountDownLatch(1);
        init();
    }

    /**
     * Method for operations that have to be done after constructor.
     */
    private void init() {
        streamActivityChecker = new StreamActivityChecker();
        executorService.submit(streamActivityChecker);
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getLastDataTimeout() {
        return lastDataTimeout;
    }

    public void setLastDataTimeout(int lastDataTimeout) {
        this.lastDataTimeout = lastDataTimeout;
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
        if (!getActiveStreams().contains(streamReceiverName)) {
            logger.warning(streamReceiverName +
                    " is not registered or inactive. Messages from this stream will not be processed.");
            return null;
        }
        streamActivityChecker.put(streamReceiverName);
        //unmarshal data
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        StringReader reader = new StringReader(input);
        Data inputData = (Data) unmarshaller.unmarshal(reader);
        //put data to sorted map
        Long timestamp = inputData.getTimestamp();
        BigDecimal amount = inputData.getAmount();
        try {
            lock.lock();
            data.merge(timestamp, amount, BigDecimal::add);
            //put to maxStreamTimestamps last timestamp
            maxStreamTimestamps.put(streamReceiverName, timestamp);
            if (outputRunner.getState() == Thread.State.NEW) {//
                executorService.submit(outputRunner);
            }
        } finally {
            lock.unlock();
        }

        //return data
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
                //all streams sent data so we can send some data to output
                Long leastMaxTimestamp =
                        maxStreamTimestamps.pollMinTimestamp();
                //prepare data for output
                Map<Long, BigDecimal> resultMap =
                        prepareData(data, leastMaxTimestamp);
                //convert to Set<Data>
                totalResult.addAll(convertData(resultMap));
                if (output.size() > 0) {
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
        if (activityStatuses.size() > 0) {
            outputLatch.await();
        }
        streamActivityChecker.shutdown();
        executorService.shutdownNow();
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
        streamActivityChecker.put(streamName);
    }

    /**
     * Close the stream (set boolean flag to false). If do not close the stream StreamCombiner will be waiting for the new data from this stream.
     *
     * @param name Name of the closed stream.
     */
    public void closeStream(String name) {
        activityStatuses.put(name, false);
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

        private MaxStreamTimestamps() {
            streamTimestamps = new HashMap<>();
            timestamps = new TreeSet<>();
        }

        private void put(String streamName, Long timestamp) {
            Long timeValue = streamTimestamps.get(streamName);
            if (timeValue != null) {
                long countTimeValue = streamTimestamps.values().stream()
                        .filter(v -> v.equals(timeValue)).count();
                //if only one such timestamp in the set than remove it for substitution it to the new value
                if (countTimeValue == 1) {
                    timestamps.remove(timeValue);
                }
            }
            streamTimestamps.put(streamName, timestamp);
            timestamps.add(timestamp);
        }

        private Long pollMinTimestamp() {
            Long result = timestamps.pollFirst();
            streamTimestamps.entrySet()
                    .removeIf(entry -> entry.getValue().equals(result));
            return result;
        }

        private Set<String> getStreamNames() {
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

    /**
     * Checks if streams hang. If it is true StreamActivityChecker sets such stream as inactive after timeout.
     */
    private class StreamActivityChecker implements Runnable {
        ConcurrentHashMap<String, Date> streamLastTimeMap =
                new ConcurrentHashMap<>();
        private boolean isActive = true;
        private int tryingAttempts = timeout / 1000;

        @Override
        public void run() {
            while (isActive && tryingAttempts > 0) {
                try {
                    if (getActiveStreams().size() == 0) {
                        logger.warning(
                                "there are no registered active streams, remaining account of the attempts - " +
                                        tryingAttempts);
                        tryingAttempts--;
                    } else {
                        tryingAttempts = timeout / 1000;
                    }
                    Thread.sleep(1000);
                    Date currenTime = new Date();
                    streamLastTimeMap.forEachEntry(1,
                            checkStreamActivity(currenTime));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void shutdown() {
            isActive = false;
        }

        /**
         * Checks how long a stream does not send data to StreamCombiner. If this time is exceed the timeout this method marks such stream as inactive.
         *
         * @param currenTime Time when that method was called.
         * @return Function for checking timeout.
         */
        private Consumer<Map.Entry<String, Date>> checkStreamActivity(
                Date currenTime) {
            return entry -> {
                Date lastTime = entry.getValue();
                if (currenTime.getTime() - lastTime.getTime() >
                        timeout) {
                    activityStatuses.put(entry.getKey(), false);
                }
            };
        }

        private void put(String stream) {
            streamLastTimeMap.put(stream, new Date());
        }
    }

}
