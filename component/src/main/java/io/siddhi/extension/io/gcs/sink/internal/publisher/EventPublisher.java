package io.siddhi.extension.io.gcs.sink.internal.publisher;


import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.PublisherObjectHolder;
import io.siddhi.extension.io.gcs.sink.internal.strategies.CountBasedRotationStrategy;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import io.siddhi.extension.io.gcs.util.ServiceClient;
import java.util.HashMap;
import java.util.Optional;
import javax.swing.plaf.nimbus.State;

public class EventPublisher {
    private ServiceClient serviceClient;
    private GCSSinkConfig config;
    private OptionHolder optionHolder;
    private RotationStrategy rotationStrategy;


    public EventPublisher(GCSSinkConfig config, OptionHolder optionHolder) {
        this.optionHolder = optionHolder;
        this.config = config;
    }

    public void initializeServiceClient() {
        serviceClient = new ServiceClient(this.config);
        rotationStrategy.setClient(serviceClient);
    }

    private RotationStrategy initRotationStrategy() {
        if (config.getFlushSize() > 0 && config.getRotateInterval() >0 ) {
            return null;
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

    public HashMap<String, PublisherObjectHolder> getEventQueue() {
        return rotationStrategy.getEventQueue();
    }

    public void setEventQueue(HashMap<String, PublisherObjectHolder> eventQueue) {
        rotationStrategy.setEventQueue(eventQueue);
    }
}
