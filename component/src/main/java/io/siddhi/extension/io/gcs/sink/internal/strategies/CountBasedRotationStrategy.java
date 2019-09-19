package io.siddhi.extension.io.gcs.sink.internal.strategies;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.StateContainer;
import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregatorFactory;
import io.siddhi.extension.io.gcs.sink.internal.publisher.PublisherTask;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Count-based rotation logic based on flushSize
 */
public class CountBasedRotationStrategy extends RotationStrategy {

    private final Logger logger = Logger.getLogger(CountBasedRotationStrategy.class);
    private Map<String, Future> countBasedFlushIntervalFutureList = new HashMap<>();


    public CountBasedRotationStrategy(GCSSinkConfig config) {
        super.setConfig(config);
    }

    @Override
    public void queueEvent(String objectName, Object event)      {
        StateContainer stateContainer = getStateContainer();

        try {
            if (stateContainer.lock()) {
                if (stateContainer.getQueuedEventMap().containsKey(objectName)) {
                    stateContainer.getQueuedEventMap().get(objectName).addEvent(event);
                    stateContainer.getEventOffsetMap().put(
                            objectName, stateContainer.getEventOffsetMap().get(objectName).intValue() + 1);

                    if (countBasedFlushIntervalFutureList.containsKey(objectName) &&
                                                        !countBasedFlushIntervalFutureList.get(objectName).isDone()) {
                        countBasedFlushIntervalFutureList.get(objectName).cancel(true);
                    }
                    scheduleFlushInterval(objectName);

                    if (stateContainer.getQueuedEventMap().get(objectName)
                            .getQueuedSize() % getConfig().getFlushSize() == 0) {
                            if (countBasedFlushIntervalFutureList.containsKey(objectName) &&
                                                        !countBasedFlushIntervalFutureList.get(objectName).isDone()) {
                            countBasedFlushIntervalFutureList.get(objectName).cancel(true);
                        }

                        String fullObjectName;

                        if (objectName.matches(String.format(".%s$", getConfig().getFileType()))) {

                            StringBuilder nameBuilder = new StringBuilder();

                            nameBuilder.append(objectName.split(String.format(".%s$", getConfig().getFileType()))[0])
                                    .append(String.format("_%s", stateContainer.getEventOffsetMap()
                                            .get(objectName).toString()))
                                    .append(String.format(".%s", getConfig().getFileType()));

                            fullObjectName = nameBuilder.toString();
                        } else {
                            fullObjectName = objectName
                                    .concat(String.format("_%s", stateContainer.getEventOffsetMap()
                                            .get(objectName).toString()))
                                    .concat(String.format(".%s", getConfig().getFileType()));
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

    public void scheduleFlushInterval(String objectName) {
        this.countBasedFlushIntervalFutureList.put(objectName, getConfig().getScheduledExecutorService()
                .schedule(new PublisherTask(objectName, getStateContainer(), getConfig(),
                        getClient()), getConfig().getFlushTimeout(), TimeUnit.MILLISECONDS));
    }

}
