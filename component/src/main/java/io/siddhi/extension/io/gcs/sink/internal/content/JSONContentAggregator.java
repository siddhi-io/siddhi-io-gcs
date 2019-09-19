package io.siddhi.extension.io.gcs.sink.internal.content;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ContentAggregator for sinks with json mapper
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
