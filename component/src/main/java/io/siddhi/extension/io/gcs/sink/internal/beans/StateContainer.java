package io.siddhi.extension.io.gcs.sink.internal.beans;

import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregator;
import org.apache.log4j.Logger;

import java.util.HashMap;

/**
 * Container class to hold the state related objects
 */
public class StateContainer {

    private static final Logger logger = Logger.getLogger(StateContainer.class);

    private HashMap<String, Integer> eventOffsetMap = new HashMap<>();
    private HashMap<String, ContentAggregator> queuedEventMap = new HashMap<>();

    private boolean isLockAcquired = false;

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

    public synchronized void getLock() {
        while (isLockAcquired) {
            try {
                wait();
            } catch (InterruptedException e) {
                logger.error("Error occurred when acquiring lock", e);
                Thread.currentThread().interrupt();
            }
        }

        isLockAcquired = true;
    }

    public synchronized void releaseLock() {
        isLockAcquired = false;
        notifyAll();
    }
}
