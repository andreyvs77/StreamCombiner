package com.client.combiner;

import com.client.model.Data;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

/**
 * Prepares data for tests
 */
public class TestData {

    private JAXBContext jaxbContext;

    private List<String> initDataServer1 = Arrays.asList(
            "<data><timestamp>1</timestamp><amount>10.0</amount></data>",
            "<data><timestamp>2</timestamp><amount>11.2</amount></data>",
            "<data><timestamp>5</timestamp><amount>11.5</amount></data>",
            "<data><timestamp>7</timestamp><amount>11.7</amount></data>",
            "<data><timestamp>8</timestamp><amount>11.8</amount></data>",
            "<data><timestamp>12</timestamp><amount>11.12</amount></data>",
            "<data><timestamp>14</timestamp><amount>11.14</amount></data>",
            "<data><timestamp>15</timestamp><amount>11.15</amount></data>",
            "<data><timestamp>16</timestamp><amount>11.16</amount></data>",
            "<data><timestamp>17</timestamp><amount>11.17</amount></data>",
            "<data><timestamp>18</timestamp><amount>11.18</amount></data>"
    );

    private List<String> initDataServer2 = Arrays.asList(
            "<data><timestamp>1</timestamp><amount>20.0</amount></data>",
            "<data><timestamp>3</timestamp><amount>21.3</amount></data>",
            "<data><timestamp>4</timestamp><amount>21.4</amount></data>",
            "<data><timestamp>7</timestamp><amount>21.7</amount></data>",
            "<data><timestamp>8</timestamp><amount>21.8</amount></data>",
            "<data><timestamp>11</timestamp><amount>21.11</amount></data>",
            "<data><timestamp>14</timestamp><amount>21.14</amount></data>",
            "<data><timestamp>15</timestamp><amount>21.15</amount></data>",
            "<data><timestamp>17</timestamp><amount>21.17</amount></data>",
            "<data><timestamp>18</timestamp><amount>21.18</amount></data>",
            "<data><timestamp>19</timestamp><amount>21.19</amount></data>"
    );

    private List<String> initDataServer3 = Arrays.asList(
            "<data><timestamp>3</timestamp><amount>31.3</amount></data>",
            "<data><timestamp>4</timestamp><amount>31.4</amount></data>",
            "<data><timestamp>5</timestamp><amount>31.7</amount></data>",
            "<data><timestamp>8</timestamp><amount>31.8</amount></data>",
            "<data><timestamp>9</timestamp><amount>31.11</amount></data>",
            "<data><timestamp>10</timestamp><amount>31.14</amount></data>",
            "<data><timestamp>11</timestamp><amount>31.15</amount></data>"
    );

    public TestData() throws JAXBException {
        this.jaxbContext = jaxbContext = JAXBContext.newInstance(Data.class);
    }

    public Data mergeDataObjects(Data first, Data second) {
        if (first.getTimestamp().equals(second.getTimestamp())) {
            return new Data(first.getTimestamp(),
                    first.getAmount().add(second.getAmount()));
        }
        return null;
    }

    public List<Data> mergeDataLists(List<Data> first, List<Data> second) {
        List<Data> result = new ArrayList<>();
        TreeMap<Long, BigDecimal> temp = new TreeMap<>();
        first.forEach(data -> {
            temp.computeIfPresent(data.getTimestamp(),
                    (key, value) -> value.add(data.getAmount()));
            temp.putIfAbsent(data.getTimestamp(), data.getAmount());
        });
        second.forEach(data -> {
            temp.computeIfPresent(data.getTimestamp(),
                    (key, value) -> value.add(data.getAmount()));
            temp.putIfAbsent(data.getTimestamp(), data.getAmount());
        });
        temp.forEach(
                (timestamp, amount) -> result.add(new Data(timestamp, amount)));
        return result;
    }

    public String getSingleMessage() {
        return initDataServer1.get(0);
    }

    public List<String> getMessageList() {
        return initDataServer1;
    }

    public List<String> getServer1Messages() {
        return initDataServer1;
    }

    public List<Data> getServer1DataList() throws JAXBException {
        return getDataList(initDataServer1);
    }

    public List<String> getServer2Messages() {
        return initDataServer2;
    }

    public List<Data> getServer2DataList() throws JAXBException {
        return getDataList(initDataServer2);
    }

    public List<String> getServer3Messages() {
        return initDataServer3;
    }

    public List<Data> getServer3DataList() throws JAXBException {
        return getDataList(initDataServer3);
    }


    public Data getSingleData(String message) throws JAXBException {
        var unmarshaller = jaxbContext.createUnmarshaller();
        var reader = new StringReader(message);
        return (Data) unmarshaller.unmarshal(reader);
    }

    public Data getSingleData() throws JAXBException {
        return getSingleData(initDataServer1.get(0));
    }

    public List<Data> getDataList(List<String> messages) throws JAXBException {
        List<Data> result = new ArrayList<>();
        for (String message : messages) {
            var unmarshaller = jaxbContext.createUnmarshaller();
            var reader = new StringReader(message);
            result.add((Data) unmarshaller.unmarshal(reader));
        }
        return result;
    }

    public List<Data> getDataList() throws JAXBException {
        return getDataList(initDataServer1);
    }


}
