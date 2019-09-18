package io.siddhi.extension.io.gcs.sink.internal.strategies;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.StateContainer;
import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregatorFactory;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import org.apache.log4j.Logger;

/**
 * Count-based rotation logic based on flushSize
 */
public class CountBasedRotationStrategy extends RotationStrategy {

    private final Logger logger = Logger.getLogger(CountBasedRotationStrategy.class);

    public CountBasedRotationStrategy(GCSSinkConfig config) {
        super.setConfig(config);
    }

    @Override
    public void queueEvent(String objectName, Object event) {
        StateContainer stateContainer = getStateContainer();

        try {
            if (stateContainer.lock()) {
                logger.info("locked");
                if (stateContainer.getQueuedEventMap().containsKey(objectName)) {
                    stateContainer.getQueuedEventMap().get(objectName).addEvent(event);
                    stateContainer.getEventOffsetMap().put(
                            objectName, stateContainer.getEventOffsetMap().get(objectName).intValue() + 1);

                    if (stateContainer.getQueuedEventMap().get(objectName)
                            .getQueuedSize() % getConfig().getFlushSize() == 0) {
                        String fullObjectName;

                        if (objectName.matches(String.format(".%s$", getConfig().getMapType()))) {
                            fullObjectName = objectName.split(String.format(".%s$", getConfig().getMapType()))[0]
                                    .concat(stateContainer.getEventOffsetMap().get(objectName).toString())
                                    .concat(String.format(".%s", getConfig().getFiltype()));
                        } else {
                            fullObjectName = objectName
                                    .concat(String.format("_%s", stateContainer.getEventOffsetMap()
                                            .get(objectName).toString()))
                                    .concat(String.format(".%s", getConfig().getFiltype()));
                        }

                        getClient().uploadObject(fullObjectName,
                                getStateContainer().getQueuedEventMap().get(objectName).getContentString());

                        stateContainer.getQueuedEventMap().put(objectName,
                                ContentAggregatorFactory.getContentGenerator(getConfig()));
                    }
                } else {
                    stateContainer.getQueuedEventMap().put(objectName,
                            ContentAggregatorFactory.getContentGenerator(getConfig()));
                    stateContainer.getQueuedEventMap().get(objectName).addEvent(event);
                    stateContainer.getEventOffsetMap().put(objectName, 1);
                }
            }
        } finally {
            stateContainer.releaseLock();
        }
    }


}
