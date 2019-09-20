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

import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import io.siddhi.extension.io.gcs.util.ServiceClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Class containing logic of publishing objects.
 */
public class EventPublisher {
    private GCSSinkConfig config;
    private OptionHolder optionHolder;
    private ServiceClient serviceClient;

    public EventPublisher(GCSSinkConfig config, OptionHolder optionHolder) {
        this.config = config;
        this.optionHolder = optionHolder;
    }

    public void initializeServiceClient() throws ConnectionUnavailableException {
        serviceClient = new ServiceClient(config).initializeClient().createBucketIfNotExists();
    }

    public void publishObject(Object payload, DynamicOptions dynamicOptions) {
        String objectName = optionHolder.validateAndGetOption(GCSConstants.OBJECT_NAME).getValue(dynamicOptions);
        String objectBody;

        switch (config.getMapType()) {
            case "avro":
                objectBody = new String(((ByteBuffer) payload).array(), StandardCharsets.UTF_8);
                break;
            case "binary":
                objectBody = new String(((ByteBuffer) payload).array(), StandardCharsets.UTF_8);
                break;
            default:
                objectBody = payload.toString();
        }

        serviceClient.uploadObject(objectName.concat(String.format(".%s",
                getFileType(config.getMapType()))), objectBody);
    }

    private String getFileType(String mapType) {
        switch (mapType) {
            case "xml":
                return "xml";
            case "json":
                return "json";
            case "text":
                return "txt";
            default:
                return "bin";

        }
    }
}
