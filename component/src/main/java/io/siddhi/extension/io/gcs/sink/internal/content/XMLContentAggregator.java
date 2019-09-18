package io.siddhi.extension.io.gcs.sink.internal.content;

/**
 * ContentAggregator for sinks with xml mapper
 */
public class XMLContentAggregator implements ContentAggregator {
    private int eventCount;
    private String enclosingElement;
    private String contentString;

    public XMLContentAggregator(String enclosingElement) {
        this.enclosingElement = enclosingElement;
        this.contentString = String.format("<%s>", enclosingElement);
    }

    @Override
    public void addEvent(Object payload) {
        eventCount++;
        contentString = contentString.concat(String.format("%n%s", payload));
    }

    @Override
    public String getContentString() {
        return contentString.concat(String.format("%n</%s>", enclosingElement));
    }

    @Override
    public int getQueuedSize() {
        return eventCount;
    }
}
