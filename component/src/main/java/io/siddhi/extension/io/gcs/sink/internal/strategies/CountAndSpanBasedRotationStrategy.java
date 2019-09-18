package io.siddhi.extension.io.gcs.sink.internal.strategies;

import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;

/**
 * Contains logic to handle count based rotation strategy with time span rotation strategy
 */
public class CountAndSpanBasedRotationStrategy extends RotationStrategy {


    @Override
    public void queueEvent(String objectName, Object event) {

    }
}
