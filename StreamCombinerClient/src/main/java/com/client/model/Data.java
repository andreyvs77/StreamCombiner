package com.client.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.math.BigDecimal;
import java.util.Objects;

/**
 * Object that represents input/output data.
 */
@XmlRootElement(name = "data")
@XmlAccessorType(XmlAccessType.FIELD)
public class Data {

    @XmlElement(name = "timestamp")
    private Long timestamp;
    @XmlElement(name = "amount")
    private BigDecimal amount;

    public Data() {
    }

    public Data(Long timestamp, BigDecimal amount) {
        this.timestamp = timestamp;
        this.amount = amount;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data = (Data) o;
        return timestamp.equals(data.timestamp) &&
                amount.equals(data.amount);
    }

    @Override public int hashCode() {
        return Objects.hash(timestamp, amount);
    }

    @Override public String toString() {
        return "Data{" +
                "timestamp=" + timestamp +
                ", amount=" + amount +
                '}';
    }
}
