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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Content Aggregator for Binary event mappers.
 */
public class BinaryContentAggregator implements ContentAggregator, Serializable {
    private int eventCount;
    private String contentDelimiter;
    private String contentString;

    public BinaryContentAggregator(String contentDelimiter) {
        this.contentDelimiter = contentDelimiter;
    }

    @Override
    public void addEvent(Object payload) {
        StringBuilder stringBuilder = new StringBuilder(contentString);

        if (eventCount > 0) {
            stringBuilder.append(String.format("%n%s%n", contentDelimiter));
        }

        stringBuilder.append(new String(((ByteBuffer) payload).array(), StandardCharsets.UTF_8));
        contentString = stringBuilder.toString();
        eventCount++;
    }

    @Override
    public String getContentString() {
        return contentString;
    }

    @Override
    public int getQueuedSize() {
        return eventCount;
    }

    public int getEventCount() {
        return eventCount;
    }

    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    public String getContentDelimiter() {
        return contentDelimiter;
    }

    public void setContentDelimiter(String contentDelimiter) {
        this.contentDelimiter = contentDelimiter;
    }

    public void setContentString(String contentString) {
        this.contentString = contentString;
    }
}
