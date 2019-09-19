package io.siddhi.extension.io.gcs.sink;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import org.testng.annotations.Test;

public class TestCaseOfGCSSink {
    
    // Before running this test provide valid GCS bucket connector information in the stream definition
    // Due to inability of testing without valid configuration against this test is commented out in the testng.xml
    @Test
    public void testSQSMessagePublisherInitialization() throws InterruptedException {
        SiddhiAppRuntime siddhiAppRuntime = null;
        SiddhiManager siddhiManager = new SiddhiManager();

        try {
            String streamDef = "@sink(type='google-cloud-storage'," +
                    "credential.provider.file.path='<absolute.file.path>'," +
                    "bucket.name='<bucket.name>'," +
                    "storage.class='MULTI_REGIONAL'," +
                    "flush.size=\"3\"," +
                    "object.name='test-object-{{suffix}}' @map(type='text') )" +
                    "define stream outStream(key string, payload string, suffix string);";

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
