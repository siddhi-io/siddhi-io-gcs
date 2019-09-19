package io.siddhi.extension.io.gcs.sink.internal.content;

import java.io.Serializable;

/**
 * ContentAggregator for sinks with Text mapper
 */
public class TextContentAggregator implements ContentAggregator, Serializable {

    private int eventCount;
    private String delimiter;
    private String contentString;

    public TextContentAggregator(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void addEvent(Object payload) {
        if (eventCount == 0) {
            contentString = payload.toString();
        } else {
            contentString = contentString.concat(String.format("%n%s%n", delimiter)).concat(payload.toString());
        }
        eventCount++;
    }

    @Override
    public String getContentString() {
        return contentString;
    }

    @Override
    public int getQueuedSize() {
        return eventCount;
    }

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setContentString(String contentString) {
        this.contentString = contentString;
    }
}
