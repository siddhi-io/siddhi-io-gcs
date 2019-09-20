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

package io.siddhi.extension.io.gcs.sink.internal.beans;

import io.siddhi.extension.io.gcs.sink.internal.content.ContentAggregator;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Container class to hold the state related objects.
 */
public class StateContainer {
    private HashMap<String, Integer> eventOffsetMap = new HashMap<>();
    private HashMap<String, ContentAggregator> queuedEventMap = new HashMap<>();

    private Lock lock = new ReentrantLock();

    public HashMap<String, Integer> getEventOffsetMap() {
        return eventOffsetMap;
    }

    public void setEventOffsetMap(HashMap<String, Integer> eventOffsetMap) {
        this.eventOffsetMap = eventOffsetMap;
    }

    public HashMap<String, ContentAggregator> getQueuedEventMap() {
        return queuedEventMap;
    }

    public void setQueuedEventMap(HashMap<String, ContentAggregator> queuedEventMap) {
        this.queuedEventMap = queuedEventMap;
    }

    public boolean lock() {
        return lock.tryLock();
    }

    public void releaseLock() {
        lock.unlock();
    }
}
