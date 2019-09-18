package io.siddhi.extension.io.gcs.sink.internal.publisher;


import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.StateContainer;
import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregator;
import io.siddhi.extension.io.gcs.sink.internal.strategies.CountAndSpanBasedRotationStrategy;
import io.siddhi.extension.io.gcs.sink.internal.strategies.CountBasedRotationStrategy;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import io.siddhi.extension.io.gcs.util.ServiceClient;
import java.util.HashMap;

/**
 * Contains the logic for Handling Event publishing to GCS
 */
public class EventPublisher {
    private ServiceClient serviceClient;
    private GCSSinkConfig config;
    private OptionHolder optionHolder;
    private RotationStrategy rotationStrategy;


    public EventPublisher(GCSSinkConfig config, OptionHolder optionHolder) {
        this.optionHolder = optionHolder;
        this.config = config;
        this.rotationStrategy = getRotationStrategy();
    }

    public void initializeServiceClient() {
        serviceClient = new ServiceClient(this.config);
        rotationStrategy.setClient(serviceClient);
    }

    private RotationStrategy getRotationStrategy() {
        if (config.getFlushSize() > 0 && config.getRotateInterval() > 0) {
            return new CountAndSpanBasedRotationStrategy();
        } else {
            return new CountBasedRotationStrategy(config);
        }
    }

    public HashMap<String, Integer> getEventOffsetMap() {
        return rotationStrategy.getEventOffsetMap();
    }

    public void setEventOffsetMap(HashMap<String, Integer> eventOffsetMap) {
        rotationStrategy.setEventOffsetMap(eventOffsetMap);
    }

    public HashMap<String, ContentAggregator> getEventQueue() {
        return rotationStrategy.getEventQueue();
    }

    public void setEventQueue(HashMap<String, ContentAggregator> eventQueue) {
        rotationStrategy.setEventQueue(eventQueue);
    }

    public StateContainer getStateContainer() {
        return rotationStrategy.getStateContainer();
    }

    public void publish(Object payload, DynamicOptions dynamicOptions) {
        String objectName = optionHolder.validateAndGetOption(GCSConstants.OBJECT_NAME).getValue(dynamicOptions);

        rotationStrategy.queueEvent(objectName, payload);
    }
}
