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

/**
 * ContentAggregator for sinks with xml mapper.
 */
public class XMLContentAggregator implements ContentAggregator, Serializable {
    private int eventCount;
    private String enclosingElement;
    private String contentString;

    public XMLContentAggregator(String enclosingElement) {
        this.enclosingElement = enclosingElement;
        this.contentString = String.format("<%s>", enclosingElement);
    }

    @Override
    public void addEvent(Object payload) {
        eventCount++;
        contentString = contentString.concat(String.format("%n%s", payload));
    }

    @Override
    public String getContentString() {
        return contentString.concat(String.format("%n</%s>", enclosingElement));
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

    public String getEnclosingElement() {
        return enclosingElement;
    }

    public void setEnclosingElement(String enclosingElement) {
        this.enclosingElement = enclosingElement;
    }

    public void setContentString(String contentString) {
        this.contentString = contentString;
    }
}
