package io.siddhi.extension.io.gcs.sink.internal.publisher;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.StateContainer;
import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregatorFactory;
import io.siddhi.extension.io.gcs.util.ServiceClient;
import org.apache.log4j.Logger;

/**
 * Runnable class to be submitted to scheduled Executor
 */
public class PublisherTask implements Runnable {

    private String objectName;
    private StateContainer stateContainer;
    private GCSSinkConfig config;
    private ServiceClient client;

    private static final Logger logger = Logger.getLogger(PublisherTask.class);

    public PublisherTask(String objectName, StateContainer stateContainer, GCSSinkConfig config,
                         ServiceClient serviceClient) {
        this.objectName = objectName;
        this.stateContainer = stateContainer;
        this.config = config;
        this.client = serviceClient;
    }

    @Override
    public void run() {

        try {
            if (stateContainer.lock()) {
                logger.info("Locked");
                String fullObjectName;

                if (objectName.matches(String.format(".%s$", config.getMapType()))) {
                    fullObjectName = objectName.split(String.format(".%s$", config.getMapType()))[0]
                            .concat(stateContainer.getEventOffsetMap().get(objectName).toString())
                            .concat(String.format(".%s", config.getFiltype()));
                } else {
                    fullObjectName = objectName
                            .concat(String.format("_%s", stateContainer.getEventOffsetMap().get(objectName).toString()))
                            .concat(String.format(".%s", config.getFiltype()));
                }

                client.uploadObject(fullObjectName, stateContainer.getQueuedEventMap()
                                                                    .get(objectName).getContentString());

                stateContainer.getQueuedEventMap().put(objectName, ContentAggregatorFactory
                                                                                .getContentGenerator(config));
            }
        } finally {
            stateContainer.releaseLock();
            logger.info("Unlocked");
        }
    }
}
