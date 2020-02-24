package com.client.combiner;

import com.client.model.Data;
import org.junit.jupiter.api.BeforeEach;
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

class StreamCombinerImplTest {

    private TestData testData;

    @BeforeEach
    private void init() throws JAXBException {
        testData = new TestData();
    }

    @Test
    public void outputData_send2DiffTimestampMessages2Streams_emptyResultAndUnsentNotEmpty()
            throws JAXBException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
        String server1Message = testData.getServer1Messages().get(0);
        String server3Message = testData.getServer3Messages().get(0);
        Data server1Data = testData.getSingleData(server1Message);
        Data server3Data = testData.getSingleData(server3Message);
        Set<Data> expectedResult = new LinkedHashSet<>();
        Set<Data> expectedUnsentResult =
                new LinkedHashSet<>(Arrays.asList(server1Data, server3Data));
        String stream1Name = "name1";
        String stream2Name = "name2";
        streamCombiner.addNewStream();
        streamCombiner.addNewStream();
        streamCombiner.addData(server1Message, stream1Name);
        streamCombiner.addData(server3Message, stream2Name);
        int expectedUnsentSize = 2;

        //Act
        Set<Data> result = streamCombiner.outputData();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
//        assertEquals(expectedResult, result);
//        assertEquals(expectedUnsentSize, unsentData.size());
//        assertIterableEquals(expectedUnsentResult, unsentData);
    }

    @Test
    public void outputData_sendMessage2Streams_emptyResultAndUnsentOne()
            throws JAXBException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
        String inputMessage = testData.getSingleMessage();
        Data data = testData.getSingleData();
        Set<Data> expectedResult = new LinkedHashSet<>();
        String streamName = "name";
        streamCombiner.addNewStream();
        streamCombiner.addNewStream();
        streamCombiner.addData(inputMessage, streamName);
        int expectedUnsentSize = 1;

        //Act
        Set<Data> result = streamCombiner.outputData();
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
    public void outputData_sendMessage1Stream_returnSetAndUnsentZero()
            throws JAXBException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
        String inputMessage = testData.getSingleMessage();
        Data data = testData.getSingleData();
        Set<Data> expectedResult = new LinkedHashSet<>(Arrays.asList(data));
        String streamName = "name";
        streamCombiner.addNewStream();
        streamCombiner.addData(inputMessage, streamName);
        int expectedUnsentSize = 0;

        //Act
        Set<Data> result = streamCombiner.outputData();
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
        assertEquals(expectedResult, result);
        assertEquals(expectedUnsentSize, unsentData.size());
    }

    @Test
    public void addData_1() {
        //Arrange
        //Act
        //Assert
    }

    @Test
    public void addData_sendSingleMessage_returnObject() throws JAXBException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
        String inputMessage = testData.getSingleMessage();
        Data expectedResult = testData.getSingleData();
        String streamName = "name";

        //Act
        Data result = streamCombiner.addData(inputMessage, streamName);
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();

        //Assert
        assertEquals(expectedResult, result);
        assertEquals(expectedResult, new Data(unsentData.firstEntry().getKey(),
                unsentData.firstEntry().getValue()));
    }

    @Test
    public void addData_sendMessageList_returnObjects() throws JAXBException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
        List<String> inputMessages = testData.getMessageList();
        List<Data> expectedResult = testData.getDataList();
        String streamName = "name";
        List<Data> result = new ArrayList<>();
        List<Data> unsentDataContent = new ArrayList<>();

        //Act
        for (String message : inputMessages) {
            result.add(streamCombiner.addData(message, streamName));
        }
        ConcurrentSkipListMap<Long, BigDecimal> unsentData =
                (ConcurrentSkipListMap<Long, BigDecimal>) streamCombiner
                        .getUnsentData();
        unsentData.forEach((timeout, amount) -> unsentDataContent
                .add(new Data(timeout, amount)));

        //Assert
        assertIterableEquals(expectedResult, result);
        assertIterableEquals(expectedResult, unsentDataContent);
    }

    @Test
    public void addData_sendMessagesInMultiThreads_returnMergedObject()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
        String server1Message = testData.getServer1Messages().get(0);
        String server2Message = testData.getServer2Messages().get(0);
        Data server1Data = testData.getSingleData(server1Message);
        Data server2Data = testData.getSingleData(server2Message);
        Data expectedUnsentResult =
                testData.mergeDataObjects(server1Data, server2Data);
        List<Data> expectedResult = Arrays.asList(server1Data, server2Data);
        String stream1Name = "name1";
        String stream2Name = "name2";
        List<Data> result;
        List<Data> resultServer1 = new ArrayList<>();
        List<Data> resultServer2 = new ArrayList<>();
        var executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        List<Data> unsentDataContent = new ArrayList<>();
        int expectedUnsentCount = 1;

        //Act
        runThread(streamCombiner, Arrays.asList(server1Message), stream1Name,
                executor, resultServer1);
        runThread(streamCombiner, Arrays.asList(server2Message), stream2Name,
                executor, resultServer2);
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
        assertTrue(unsentDataContent.contains(expectedUnsentResult));
        assertEquals(expectedUnsentCount, unsentDataContent.size());

    }

    @Test
    public void addData_sendMessageListsInMultiThreads_returnObjects()
            throws JAXBException, InterruptedException {
        //Arrange
        StreamCombinerImpl streamCombiner = new StreamCombinerImpl();
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
        List<Data> expectedUnsentResult =
                testData.mergeDataLists(
                        testData.mergeDataLists(server1DataList,
                                server2DataList),
                        server3DataList);
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
        runThread(streamCombiner, server1Messages, stream1Name, executor,
                resultServer1);
        runThread(streamCombiner, server2Messages, stream2Name, executor,
                resultServer2);
        runThread(streamCombiner, server3Messages, stream3Name, executor,
                resultServer3);
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

    private void runThread(StreamCombinerImpl streamCombiner,
                           List<String> serverMessages, String streamName,
                           ThreadPoolExecutor executor,
                           List<Data> result) {
        executor.execute(() -> {
            for (String message : serverMessages) {
                try {
                    result.add(streamCombiner.addData(message, streamName));
                } catch (JAXBException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

}