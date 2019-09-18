package io.siddhi.extension.io.gcs.sink.internal.content;

/**
 * ContentAggregator for sinks with text mapper
 */
public class TextContentAggregator implements ContentAggregator {

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
}
