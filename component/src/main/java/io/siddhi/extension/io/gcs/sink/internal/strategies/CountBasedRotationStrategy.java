package io.siddhi.extension.io.gcs.sink.internal.strategies;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import io.siddhi.extension.io.gcs.util.ServiceClient;
import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 * Countbased rotation logic based on flushSize
 */
public class CountBasedRotationStrategy extends RotationStrategy {

    private final Logger logger = Logger.getLogger(CountBasedRotationStrategy.class);

    public CountBasedRotationStrategy(GCSSinkConfig config) {
        super.setConfig(config);
    }

    @Override
    public void queueEvent(String objectName, Object event) {

    }


}
