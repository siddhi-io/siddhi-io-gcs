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

package io.siddhi.extension.io.gcs.sink.internal.content;

import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;

/**
 * Class to initialize ContentAggregators.
 */
public class ContentAggregatorFactory {

    public static ContentAggregator getContentGenerator(GCSSinkConfig config) {
        switch (config.getMapType().toLowerCase()) {
            case "json":
                return new JSONContentAggregator();
            case "xml":
                return new XMLContentAggregator(config.getEnclosingElement());
            case "text":
                return new TextContentAggregator(config.getTextDelimiter());
            case "binary":
                return new BinaryContentAggregator(config.getTextDelimiter());
            case "avro":
                return new BinaryContentAggregator(config.getTextDelimiter());
            default:
                // not a supported ContentAggregator
                return null;
        }
    }

    private ContentAggregatorFactory() {
        // to stop this class from getting initialized.
    }

}
