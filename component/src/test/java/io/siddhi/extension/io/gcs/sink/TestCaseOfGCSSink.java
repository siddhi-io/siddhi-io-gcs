package io.siddhi.extension.io.gcs.sink;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import org.testng.annotations.Test;

/**
 * Class containing testcases for GCS Sink.
 */
public class TestCaseOfGCSSink {
    @Test
    public void testSQSMessagePublisherInitialization() throws InterruptedException {
        SiddhiAppRuntime siddhiAppRuntime = null;
        SiddhiManager siddhiManager = new SiddhiManager();

        try {
            String streamDef = "@sink(type='google-cloud-storage'," +
                    " credential.path='<auth.file.path>'," +
                    " bucket.name='<bucket.name>'," +
                    " object.name='test-object-{{suffix}}',  @map(type='text') ) \n" +
                    "define stream outputStream(key string, payload string, suffix string);";

            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streamDef);
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("outStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"message 1", "TestContent", "suffix"});
        } finally {
            if (siddhiAppRuntime != null) {
                siddhiAppRuntime.shutdown();
            }
        }
    }
}
