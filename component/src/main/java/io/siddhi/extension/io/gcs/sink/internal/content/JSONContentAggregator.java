package io.siddhi.extension.io.gcs.sink.internal.content;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.List;

/**
 * ContentAggregator for sinks with json mapper
 */
public class JSONContentAggregator implements ContentAggregator {
    private List<JsonObject> eventList = new ArrayList<>();

    @Override
    public void addEvent(Object payload) {
        JsonObject jsonObject = new JsonParser().parse(payload.toString()).getAsJsonObject();
        eventList.add(jsonObject);
    }

    @Override
    public String getContentString() {
        return new Gson().toJson(eventList);
    }

    @Override
    public int getQueuedSize() {
        return eventList.size();
    }
}
