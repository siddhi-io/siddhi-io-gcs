package io.siddhi.extension.io.gcs.sink.internal.strategies;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregatorFactory;
import io.siddhi.extension.io.gcs.sink.internal.publisher.PublisherTask;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import org.apache.log4j.Logger;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class to handle span based object rotation
 */
public class RotateIntervalStrategy extends RotationStrategy {

    private GCSSinkConfig config;
    private ScheduledFuture future;

    private static final Logger logger = Logger.getLogger(RotateIntervalStrategy.class);

    public RotateIntervalStrategy(GCSSinkConfig config) {
        this.config = config;
    }

    @Override
    public void queueEvent(String objectName, Object event) {


        try {
            getStateContainer().lock();

            if (getStateContainer().getQueuedEventMap().containsKey(objectName)) {

                getStateContainer().getQueuedEventMap().get(objectName).addEvent(event);
                getStateContainer().getEventOffsetMap().put(objectName,
                        getStateContainer().getEventOffsetMap().get(objectName).intValue() + 1);


            } else {
                getStateContainer().getQueuedEventMap().put(objectName,
                        ContentAggregatorFactory.getContentGenerator(config));
                getStateContainer().getQueuedEventMap().get(objectName).addEvent(event);
                getStateContainer().getEventOffsetMap().put(objectName, Integer.valueOf(1));
            }
        } finally {
            getStateContainer().releaseLock();
        }

        if (future == null || future.isDone()) {
            try {
                future = config.getScheduledExecutorService()
                        .schedule(new PublisherTask(objectName, getStateContainer(), config,
                                getClient()), config.getRotateInterval(), TimeUnit.MILLISECONDS);
            } catch (NullPointerException e) {
                logger.error("error", e);
            }


        }

    }
}
