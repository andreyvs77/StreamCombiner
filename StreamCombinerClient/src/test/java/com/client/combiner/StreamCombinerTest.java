package com.client.combiner;

import com.client.model.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBException;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class StreamCombinerTest {

    private TestData testData;

    @BeforeEach
    private void init() throws JAXBException {
        testData = new TestData();
    }

    @Test
    public void process_checkStreamsHang_excludeStreamForActive()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        streamCombiner.setTimeout(1000);
        List<String> server1Messages = testData.getServer1Messages();
        List<String> server2Messages = testData.getServer2Messages();
        List<Data> server1DataList = testData.getServer1DataList();
        List<Data> server2DataList = testData.getServer2DataList();
        String stream1Name = "name1";
        String stream2Name = "name2";
        streamCombiner.addNewStream(stream1Name);
        streamCombiner.addNewStream(stream2Name);
        //expected result
        List<Data> expectedResult = testData.mergeDataLists(
                server1DataList.subList(0, 4),
                server2DataList.subList(0, 4));
        expectedResult
                .addAll(server1DataList.subList(4, server2DataList.size()));


        //Act
        for (String message : server1Messages) {
            streamCombiner.process(message, stream1Name);
        }
        for (int i = 0; i < server2Messages.size(); i++) {
            if (i == 4) {
                Thread.sleep(2000);
            }
            streamCombiner.process(server2Messages.get(i), stream2Name);
        }
        streamCombiner.closeStream(stream1Name);
        streamCombiner.closeStream(stream2Name);
        LinkedHashSet<Data> result = streamCombiner.getTotalResult();
        streamCombiner.shutdown();

        //Assert
        assertIterableEquals(expectedResult, result);
    }

    @RepeatedTest(10)
    public void process_sendMessages3StreamsSimultaneously_ResultCombinedAndUnsentEmpty()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();

        List<String> server1Messages = testData.getServer1Messages();
        List<String> server2Messages = testData.getServer2Messages();
        List<String> server3Messages = testData.getServer3Messages();
        List<Data> server1DataList = testData.getServer1DataList();
        List<Data> server2DataList = testData.getServer2DataList();
        List<Data> server3DataList = testData.getServer3DataList();

        //this data will be send
        Set<Data> expectedResult = new LinkedHashSet<>(
                testData.mergeDataLists(
                        testData.mergeDataLists(server1DataList,
                                server2DataList),
                        server3DataList));

        String stream1Name = "name1";
        String stream2Name = "name2";
        String stream3Name = "name3";
        streamCombiner.addNewStream(stream1Name);
        streamCombiner.addNewStream(stream2Name);
        streamCombiner.addNewStream(stream3Name);

        var executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(3);
        int expectedUnsentSize = 0;

        //Act
        runProcessInThread(streamCombiner, server1Messages, stream1Name,
                executor, new LinkedHashSet<>());
        runProcessInThread(streamCombiner, server2Messages, stream2Name,
                executor, new LinkedHashSet<>());
        runProcessInThread(streamCombiner, server3Messages, stream3Name,
                executor, new LinkedHashSet<>());

        streamCombiner.shutdown();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();
        System.out.println(unsentData);
        LinkedHashSet<Data> totalResult = streamCombiner.getTotalResult();

        //Assert
        assertIterableEquals(expectedResult, totalResult);
        assertEquals(expectedUnsentSize, unsentData.size());
    }

    @Test
    public void process_sendMessages3StreamsConsecutively_ResultCombinedAndUnsentEmpty()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();

        List<String> server1Messages = testData.getServer1Messages();
        List<String> server2Messages = testData.getServer2Messages();
        List<String> server3Messages = testData.getServer3Messages();
        List<Data> server1DataList = testData.getServer1DataList();
        List<Data> server2DataList = testData.getServer2DataList();
        List<Data> server3DataList = testData.getServer3DataList();

        //this data will be send
        Set<Data> expectedResult = new LinkedHashSet<>(
                testData.mergeDataLists(
                        testData.mergeDataLists(server1DataList,
                                server2DataList),
                        server3DataList));

        String stream1Name = "name1";
        String stream2Name = "name2";
        String stream3Name = "name3";
        streamCombiner.addNewStream(stream1Name);
        streamCombiner.addNewStream(stream2Name);
        streamCombiner.addNewStream(stream3Name);
        int expectedUnsentSize = 0;

        //Act
        addDataList(streamCombiner, server1Messages, stream1Name);
        addDataList(streamCombiner, server2Messages, stream2Name);
        addDataList(streamCombiner, server3Messages, stream3Name);

        Set<Data> result = streamCombiner.getTotalResult();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        streamCombiner.shutdown();

        //Assert
        assertIterableEquals(expectedResult, result);
        assertEquals(expectedUnsentSize, unsentData.size());
    }

    private void addDataList(StreamCombiner streamCombiner,
                             List<String> serverMessages, String streamName)
            throws JAXBException {
        for (String message : serverMessages) {
            streamCombiner.process(message, streamName);
        }
        streamCombiner.closeStream(streamName);
    }

    @Test
    public void process_send2SameTimestampMessages2Streams_ResultCombinedAndUnsentEmpty()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        String server1Message = testData.getServer1Messages().get(0);
        String server2Message = testData.getServer2Messages().get(0);
        Data server1Data = testData.getSingleData(server1Message);
        Data server2Data = testData.getSingleData(server2Message);
        //this data will be send
        Data expectedResult =
                testData.mergeDataObjects(server1Data, server2Data);
        int expectedResultSize = 1;
        String stream1Name = "name1";
        String stream2Name = "name2";
        streamCombiner.addNewStream(stream1Name);
        streamCombiner.addNewStream(stream2Name);
        int expectedUnsentSize = 0;

        //Act
        streamCombiner.process(server1Message, stream1Name);
        streamCombiner.process(server2Message, stream2Name);
        streamCombiner.closeStream(stream1Name);
        streamCombiner.closeStream(stream2Name);
        streamCombiner.shutdown();
        Set<Data> result = streamCombiner.getTotalResult();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
        assertEquals(expectedResultSize, result.size());
        assertTrue(result.contains(expectedResult));
        assertEquals(expectedUnsentSize, unsentData.size());
    }

    @Test
    public void process_send2DiffTimestampMessages2Streams_ResultNotEmptyAndUnsentEmpty()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        String server1Message = testData.getServer1Messages().get(0);
        String server3Message = testData.getServer3Messages().get(0);
        Data server1Data = testData.getSingleData(server1Message);
        Data server3Data = testData.getSingleData(server3Message);
        //this data will be send
        Set<Data> expectedResult =
                new LinkedHashSet<>(Arrays.asList(server1Data, server3Data));
        //this data will remain unsent
        Set<Data> expectedUnsentResult = new LinkedHashSet<>();
        Set<Data> unsentResult = new LinkedHashSet<>();
        String stream1Name = "name1";
        String stream2Name = "name2";
        streamCombiner.addNewStream(stream1Name);
        streamCombiner.addNewStream(stream2Name);
        int expectedUnsentSize = 0;

        //Act
        streamCombiner.process(server1Message, stream1Name);
        streamCombiner.process(server3Message, stream2Name);
        streamCombiner.closeStream(stream1Name);
        streamCombiner.closeStream(stream2Name);
        streamCombiner.shutdown();
        Set<Data> result = streamCombiner.getTotalResult();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();
        unsentData.forEach((timestamp, amount) -> unsentResult
                .add(new Data(timestamp, amount)));

        //Assert
        assertEquals(expectedResult, result);
        assertEquals(expectedUnsentSize, unsentData.size());
        assertIterableEquals(expectedUnsentResult, unsentResult);
    }

    @Test
    public void process_sendMessage2Streams_emptyResultAndUnsentOne()
            throws JAXBException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        String inputMessage = testData.getSingleMessage();
        Data data = testData.getSingleData();
        Set<Data> expectedResult = new LinkedHashSet<>();
        String streamName = "name";
        streamCombiner.addNewStream(streamName);
        streamCombiner.addNewStream("name2");
        int expectedUnsentSize = 1;

        //Act
        streamCombiner.process(inputMessage, streamName);
        Set<Data> result = streamCombiner.getTotalResult();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
        assertEquals(expectedResult, result);
        assertEquals(expectedUnsentSize, unsentData.size());
        assertTrue(unsentData.containsKey(data.getTimestamp()) &&
                unsentData.containsValue(data.getAmount()));
    }

    @Test
    public void process_sendMessage1Stream_returnSetAndUnsentZero()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        String inputMessage = testData.getSingleMessage();
        Data data = testData.getSingleData();
        Set<Data> expectedResult = new LinkedHashSet<>(Arrays.asList(data));
        String streamName = "name";
        streamCombiner.addNewStream(streamName);
        int expectedUnsentSize = 0;

        //Act
        streamCombiner.process(inputMessage, streamName);
        streamCombiner.closeStream(streamName);
        streamCombiner.shutdown();
        Set<Data> result = streamCombiner.getTotalResult();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
        assertEquals(expectedResult, result);
        assertEquals(expectedUnsentSize, unsentData.size());
    }

    @Test
    public void process_sendSingleMessage_returnObject() throws JAXBException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        String inputMessage = testData.getSingleMessage();
        Data expectedResult = testData.getSingleData();
        String streamName = "name";
        streamCombiner.addNewStream(streamName);

        //Act
        Data result = streamCombiner.process(inputMessage, streamName);
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
        assertEquals(expectedResult, result);
        assertEquals(expectedResult, new Data(unsentData.firstEntry().getKey(),
                unsentData.firstEntry().getValue()));
    }

    @Test
    public void process_sendMessageList_returnObjects()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        List<String> inputMessages = testData.getMessageList();
        List<Data> expectedResult = testData.getDataList();
        String streamName = "name";
        streamCombiner.addNewStream(streamName);
        List<Data> result = new ArrayList<>();
        List<Data> unsentDataContent = new ArrayList<>();

        //Act
        for (String message : inputMessages) {
            result.add(streamCombiner.process(message, streamName));
        }
        streamCombiner.closeStream(streamName);
        streamCombiner.shutdown();

        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();
        unsentData.forEach((timeout, amount) -> unsentDataContent
                .add(new Data(timeout, amount)));

        //Assert
        assertIterableEquals(expectedResult, result);
        assertTrue(unsentDataContent.isEmpty());
    }

    @Test
    public void process_sendMessagesInMultiThreads_returnMergedObject()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        String server1Message = testData.getServer1Messages().get(0);
        String server2Message = testData.getServer2Messages().get(0);
        Data server1Data = testData.getSingleData(server1Message);
        Data server2Data = testData.getSingleData(server2Message);
        List<Data> expectedResult = Arrays.asList(server1Data, server2Data);
        String stream1Name = "name1";
        String stream2Name = "name2";
        streamCombiner.addNewStream(stream1Name);
        streamCombiner.addNewStream(stream2Name);
        List<Data> result;
        List<Data> resultServer1 = new ArrayList<>();
        List<Data> resultServer2 = new ArrayList<>();
        var executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        List<Data> unsentDataContent = new ArrayList<>();

        //Act
        runAddDataInThread(streamCombiner, Arrays.asList(server1Message),
                stream1Name,
                executor, resultServer1);
        runAddDataInThread(streamCombiner, Arrays.asList(server2Message),
                stream2Name,
                executor, resultServer2);

        streamCombiner.shutdown();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        result = Stream.of(
                resultServer1, resultServer2)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();
        unsentData.forEach((timeout, amount) -> unsentDataContent
                .add(new Data(timeout, amount)));

        //Assert
        assertIterableEquals(expectedResult, result);
        assertTrue(unsentDataContent.isEmpty());
    }

    @Test
    public void process_sendMessageListsInMultiThreads_returnObjects()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombiner streamCombiner = new StreamCombiner();
        List<String> server1Messages = testData.getServer1Messages();
        List<String> server2Messages = testData.getServer2Messages();
        List<String> server3Messages = testData.getServer3Messages();
        List<Data> server1DataList = testData.getServer1DataList();
        List<Data> server2DataList = testData.getServer2DataList();
        List<Data> server3DataList = testData.getServer3DataList();
        List<Data> expectedResult = Stream.of(
                server1DataList, server2DataList, server3DataList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<Data> expectedUnsentResult = new ArrayList<>();
        String stream1Name = "name1";
        String stream2Name = "name2";
        String stream3Name = "name3";
        List<Data> result;
        List<Data> resultServer1 = new ArrayList<>();
        List<Data> resultServer2 = new ArrayList<>();
        List<Data> resultServer3 = new ArrayList<>();
        List<Data> unsentDataContent = new ArrayList<>();
        var executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

        //Act
        runAddDataInThread(streamCombiner, server1Messages, stream1Name,
                executor,
                resultServer1);
        runAddDataInThread(streamCombiner, server2Messages, stream2Name,
                executor,
                resultServer2);
        runAddDataInThread(streamCombiner, server3Messages, stream3Name,
                executor,
                resultServer3);
        streamCombiner.shutdown();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        result = Stream.of(
                server1DataList, server2DataList, server3DataList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();
        unsentData.forEach((timeout, amount) -> unsentDataContent
                .add(new Data(timeout, amount)));

        //Assert
        assertIterableEquals(expectedResult, result);
        assertIterableEquals(expectedUnsentResult, unsentDataContent);
    }

    private void runAddDataInThread(StreamCombiner streamCombiner,
                                    List<String> serverMessages,
                                    String streamName,
                                    ThreadPoolExecutor executor,
                                    List<Data> result) {
        executor.execute(() -> {
            for (String message : serverMessages) {
                try {
                    result.add(streamCombiner.process(message, streamName));
                } catch (JAXBException e) {
                    throw new RuntimeException(e);
                }
            }
            streamCombiner.closeStream(streamName);
        });
    }

    private void runProcessInThread(StreamCombiner streamCombiner,
                                    List<String> serverMessages,
                                    String streamName,
                                    ThreadPoolExecutor executor,
                                    Set<Data> result) {
        executor.execute(() -> {
            for (String message : serverMessages) {
                try {
                    streamCombiner.process(message, streamName);
                    System.out.println(message);
                } catch (JAXBException e) {
                    throw new RuntimeException(e);
                }
            }

            streamCombiner.closeStream(streamName);
            System.out.println("removed - " + streamName);
        });
    }

}