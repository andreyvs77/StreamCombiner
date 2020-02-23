package com.client.combiner;

public interface StreamReceiver extends Runnable{

    void receiveData();

    void notifyAboutTimeout();
}
