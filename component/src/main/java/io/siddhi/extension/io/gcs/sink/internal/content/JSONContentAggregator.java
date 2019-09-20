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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ContentAggregator for sinks with json mapper.
 */
public class JSONContentAggregator implements ContentAggregator, Serializable {
    private ArrayList<Object> eventList = new ArrayList<>();

    @Override
    public void addEvent(Object payload) {
        eventList.add(payload);
    }

    @Override
    public String getContentString() {
        List<JsonObject> jsonObjects = new ArrayList<>();

        eventList.forEach(e -> {
            jsonObjects.add(new JsonParser().parse(e.toString()).getAsJsonObject());
        });

        return new Gson().toJson(eventList);
    }

    @Override
    public int getQueuedSize() {
        return eventList.size();
    }

    public ArrayList<Object> getEventList() {
        return eventList;
    }

    public void setEventList(ArrayList<Object> eventList) {
        this.eventList = eventList;
    }
}
