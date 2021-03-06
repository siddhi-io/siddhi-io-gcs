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
