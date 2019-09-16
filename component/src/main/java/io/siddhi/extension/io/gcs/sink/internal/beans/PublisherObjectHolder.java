package io.siddhi.extension.io.gcs.sink.internal.beans;

import java.util.List;

public class PublisherObjectHolder<T> {

    private List<T> events;
    private GCSSinkConfig config;
    private String objectName;

    public PublisherObjectHolder(GCSSinkConfig config, String objectName) {
        this.config = config;
        this.objectName = objectName;
    }


}
