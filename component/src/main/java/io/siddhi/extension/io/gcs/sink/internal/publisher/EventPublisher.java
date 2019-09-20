/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.siddhi.extension.io.gcs.sink.internal.publisher;

import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.StateContainer;
import io.siddhi.extension.io.gcs.sink.internal.strategies.CountBasedRotationStrategy;
import io.siddhi.extension.io.gcs.sink.internal.strategies.RotateIntervalStrategy;
import io.siddhi.extension.io.gcs.sink.internal.util.RotationStrategy;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import io.siddhi.extension.io.gcs.util.ServiceClient;

/**
 * Contains the logic for Handling Event publishing to GCS.
 */
public class EventPublisher {
    private GCSSinkConfig config;
    private OptionHolder optionHolder;
    private RotationStrategy rotationStrategy;

    public EventPublisher(GCSSinkConfig config, OptionHolder optionHolder) {
        this.optionHolder = optionHolder;
        this.config = config;
        this.rotationStrategy = getRotationStrategy();
    }

    public void initializeServiceClient() {
        ServiceClient serviceClient = new ServiceClient(this.config);
        rotationStrategy.setClient(serviceClient);
    }

    private RotationStrategy getRotationStrategy() {
        if (config.getRotateInterval() > 0) {
            return new RotateIntervalStrategy(config);
        } else {
            return new CountBasedRotationStrategy(config);
        }
    }

    public StateContainer getStateContainer() {
        return rotationStrategy.getStateContainer();
    }

    public void publish(Object payload, DynamicOptions dynamicOptions) {
        String objectName = optionHolder.validateAndGetOption(GCSConstants.OBJECT_NAME).getValue(dynamicOptions);
        rotationStrategy.queueEvent(objectName, payload);
    }
}
