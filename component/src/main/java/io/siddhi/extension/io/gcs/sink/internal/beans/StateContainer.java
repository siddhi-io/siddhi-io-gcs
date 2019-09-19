package io.siddhi.extension.io.gcs.sink.internal.beans;

import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregator;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Container class to hold the state related objects
 */
public class StateContainer {


    private HashMap<String, Integer> eventOffsetMap = new HashMap<>();
    private HashMap<String, ContentAggregator> queuedEventMap = new HashMap<>();

    private Lock lock = new ReentrantLock();

    public HashMap<String, Integer> getEventOffsetMap() {
        return eventOffsetMap;
    }

    public void setEventOffsetMap(HashMap<String, Integer> eventOffsetMap) {
        this.eventOffsetMap = eventOffsetMap;
    }

    public HashMap<String, ContentAggregator> getQueuedEventMap() {
        return queuedEventMap;
    }

    public void setQueuedEventMap(HashMap<String, ContentAggregator> queuedEventMap) {
        this.queuedEventMap = queuedEventMap;
    }

    public boolean lock() {
        return lock.tryLock();
    }

    public void releaseLock() {
        lock.unlock();
    }
}
