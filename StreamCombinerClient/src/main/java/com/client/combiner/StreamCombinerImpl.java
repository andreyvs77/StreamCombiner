package com.client.combiner;

import com.client.model.Data;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class StreamCombinerImpl implements StreamCombiner<Data> {

    private static final Logger logger =
            Logger.getLogger(StreamCombinerImpl.class.getName());

    private JAXBContext jaxbContext;
    private ConcurrentSkipListMap<Long, BigDecimal> data;
    private ConcurrentSkipListMap<DataDto, Long> streamMaxTimestamps;
    private AtomicInteger streamCount = new AtomicInteger();
    private final ReentrantLock lock = new ReentrantLock();

    public StreamCombinerImpl() throws JAXBException {
        jaxbContext = JAXBContext.newInstance(Data.class);
        data = new ConcurrentSkipListMap<>();
        streamMaxTimestamps = new ConcurrentSkipListMap<>();
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
        try{
            lock.lock();
            data.computeIfPresent(timestamp, (key, value) -> value.add(amount));
            data.putIfAbsent(timestamp, amount);
        }
        finally {
            lock.unlock();
        }

        //put to streamMaxTimestamps last timestamp
        streamMaxTimestamps
                .put(new DataDto(streamReceiverName, timestamp), timestamp);
        //return data
        logger.fine(inputData.toString());
        return inputData;
    }

    @Override
    public Set<Data> outputData() {
        Set<Data> result = new LinkedHashSet<>();
        if (streamCount.get() == streamMaxTimestamps.size()) {
            logger.info("streamCount.get() - "+streamCount.get()+" = "+"streamMaxTimestamps.size() - "+streamMaxTimestamps.size());
            //all streams sent data so we can send some data to output
            Long leastMaxTimestamp =
                    streamMaxTimestamps.firstEntry().getValue();
            System.out.println("leastMaxTimestamp - "+leastMaxTimestamp);
            try {
                lock.lock();
                System.out.println("before - "+data.size());
                data.forEach((k,v)->System.out.println(k+"="+v));
                System.out.println("==before");
                //prepare data for output
                Map<Long, BigDecimal> resultMap =
                        prepareData(data, leastMaxTimestamp);
                //convert to Set<Data>
                result = convertData(resultMap);
                System.out.println("after");
                data.forEach((k,v)->System.out.println(k+"="+v));
                System.out.println("after==");
                if (result.size() > 0) {
                    sendDataToStdOut(result);
                }
            } finally {
                lock.unlock();
            }
        }
        return result;
    }

    private void sendDataToStdOut(Set<Data> result) {
        logger.info(Thread.currentThread().getName()+ " send data do standart output :");
        result.forEach(System.out::println);
    }

    private Set<Data> convertData(Map<Long, BigDecimal> input) {
        Set<Data> result = new LinkedHashSet<>();
        input.forEach(
                (timestamp, amount) -> result.add(new Data(timestamp, amount)));
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
                takeWhile(n -> maxTimestamp.compareTo(n.getKey())>=0).
                forEach(e -> {
                    System.out.println("---"+e);
                    result.put(e.getKey(), e.getValue());
                    input.remove(e.getKey());
                });
//        if (input.firstEntry() != null) {
//            result.put(input.firstEntry().getKey(),
//                    input.firstEntry().getValue());
//            input.remove(input.firstEntry().getKey());
//        }
        return result;
    }

    @Override
    public int addNewStream() {
        return streamCount.incrementAndGet();
    }

    @Override
    public int removeStream(String name) {
        streamMaxTimestamps.remove(name);
        return streamCount.decrementAndGet();
    }

    @Override
    public Map<Long, BigDecimal> getUnsentData() {
        return data;
    }

    private static class DataDto implements Comparable<DataDto> {

        private String name;
        private Long timestamp;

        public DataDto() {
        }

        public DataDto(String name, Long timestamp) {
            this.name = name;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(DataDto o) {
            if (name.equals(o.name)) {
                return 0;
            }
            if (timestamp > o.timestamp) {
                return 1;
            }
            return -1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataDto dataDto = (DataDto) o;
            return name.equals(dataDto.name) &&
                    timestamp.equals(dataDto.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, timestamp);
        }

        @Override public String toString() {
            return "DataDto{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }
}
