package io.siddhi.extension.io.gcs.sink.internal.content;

import java.io.Serializable;

/**
 * ContentAggregator for sinks with xml mapper
 */
public class XMLContentAggregator implements ContentAggregator, Serializable {
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

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    public String getEnclosingElement() {
        return enclosingElement;
    }

    public void setEnclosingElement(String enclosingElement) {
        this.enclosingElement = enclosingElement;
    }

    public void setContentString(String contentString) {
        this.contentString = contentString;
    }
}
