package io.siddhi.extension.io.gcs.sink.internal.content;

/**
 * Interface for ContentAggregators
 */
public interface ContentAggregator {

    void addEvent(Object payload);

    String getContentString();

    int getQueuedSize();
}
