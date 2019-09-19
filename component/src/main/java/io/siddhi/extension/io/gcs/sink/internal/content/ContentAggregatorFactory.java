package io.siddhi.extension.io.gcs.sink.internal.content;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;

/**
 * Class to initialize ContentAggregators
 */
public class ContentAggregatorFactory {

    public static ContentAggregator getContentGenerator(GCSSinkConfig config) {
        switch (config.getMapType().toLowerCase()) {
            case "json":
                return new JSONContentAggregator();
            case "xml":
                return new XMLContentAggregator(config.getEnclosingElement());
            case "text":
                return new TextContentAggregator(config.getTextDelimiter());
            case "binary":
                return new BinaryContentAggregator(config.getTextDelimiter());
            case "avro":
                return new BinaryContentAggregator(config.getTextDelimiter());
            default:
                // not a supported ContentAggregator
                return null;
        }
    }

    private ContentAggregatorFactory() {
        // to stop this class from getting initialized.
    }

}
