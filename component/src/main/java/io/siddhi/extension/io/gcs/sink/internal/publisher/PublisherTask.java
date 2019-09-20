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

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.beans.StateContainer;
import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregatorFactory;
import io.siddhi.extension.io.gcs.util.ServiceClient;
import org.apache.log4j.Logger;

/**
 * Runnable class to be submitted to scheduled Executor.
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

                StringBuilder stringBuilder = new StringBuilder();

                if (objectName.matches(String.format(".%s$", config.getMapType()))) {

                    stringBuilder.append(objectName.split(String.format(".%s$", config.getMapType()))[0])
                                 .append(String.format("_%s", stateContainer.getEventOffsetMap()
                                                                            .get(objectName).toString()))
                                 .append(String.format(".%s", config.getFileType()));
                } else {
                    stringBuilder.append(objectName)
                                 .append(String.format("_%s", stateContainer.getEventOffsetMap()
                                                                                    .get(objectName).toString()))
                                 .append(String.format(".%s", config.getFileType()));
                }

                client.uploadObject(stringBuilder.toString(), stateContainer.getQueuedEventMap()
                                                                    .get(objectName).getContentString());

                stateContainer.getQueuedEventMap().put(objectName, ContentAggregatorFactory
                                                                                .getContentGenerator(config));
            }
        }  finally {
            stateContainer.releaseLock();
        }
    }
}
