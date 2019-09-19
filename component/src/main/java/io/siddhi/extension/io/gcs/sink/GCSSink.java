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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import io.siddhi.extension.io.gcs.sink.internal.publisher.EventPublisher;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * GCS Sink class
 */

@Extension(
        name = "google-cloud-storage",
        namespace = "sink",
        description = "Sink extension which can be used to publish events to a GCS bucket.",
        parameters = {
                @Parameter(
                        name = GCSConstants.BUCKET_NAME,
                        type = DataType.STRING,
                        description = "Name of the GCS bucket"
                ),
                @Parameter(
                        name = GCSConstants.CREDENTIAL_FILE_PATH,
                        type = DataType.STRING,
                        description = "Absolute path for the location of the authentication file obtained through the" +
                                " Google Cloud Platform Console",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = GCSConstants.ENABLE_VERSIONING,
                        type = DataType.BOOL,
                        optional = true,
                        defaultValue = "false",
                        description = "Boolean option to indicate whether the bucket should enable versioning or not"
                ),
                @Parameter(
                        name = GCSConstants.STORAGE_CLASS,
                        type = DataType.STRING,
                        description = "Storage class of the objects that are stored in the bucket possible values " +
                                "are, `multi-regional`, `regional`, 'nearline', `coldline`"
                ),
                @Parameter(
                        name = GCSConstants.CONTENT_TYPE,
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "text/plain",
                        description = "Type of the objects written to the bucket"

                ),
                @Parameter(
                        name = GCSConstants.BUCKET_ACL,
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "EMPTY_STRING",
                        description = "Access Control List for the bucket level ACL defined as a key value pair list" +
                                " defined as \"'<key>:<value>','<key>:<value>'\""
                ),
                @Parameter(
                        name = GCSConstants.OBJECT_NAME,
                        type = DataType.STRING,
                        dynamic = true,
                        description = "Full name of the object given to the object including the path"
                ),
                @Parameter(
                        name = GCSConstants.FLUSH_SIZE,
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "1",
                        description = "Number of events that the sink will wait before making a file commit"
                ),
                @Parameter(
                        name = GCSConstants.ROTATE_INTERVAL_MS,
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "-1",
                        description = "Maximum Time Span an Object should be kept open to add events before writing a" +
                                " file commit in milliseconds, This option cannot be used with when flush.size is" +
                                " enabled"
                ),
                @Parameter(
                        name = GCSConstants.ENCLOSING_ELEMENT,
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = GCSConstants.DEFAULT_ENCLOSING_ELEMENT,
                        description = "Enclosing element to contain the events in case an xml mapper is used"
                ),
                @Parameter(
                        name = GCSConstants.TEXT_DELIMITER,
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = GCSConstants.DEFAULT_TEXT_DELIMITER,
                        description = "Delimiter to be used as event separator when text/binary mapper is used"
                ),
                @Parameter(
                        name = GCSConstants.FLUSH_TIMEOUT,
                        type = DataType.INT,
                        optional = true,
                        defaultValue = GCSConstants.DEFAULT_FLUSH_TIMEOUT,
                        description = "Timeout that the sink should wait before making a file commit when Flush " +
                                "Size option is enabled"
                )
        },
        examples = {
                @Example(
                        syntax = "@sink(type='google-cloud-storage'," +
                                "credential.provider.file.path='/Users/charukak/Downloads/mfp-f1a16bac21ad.json'," +
                                "bucket.name='charukak-bucket'," +
                                "storage.class='multi-regional'," +
                                "flush.size=\"3\"," +
                                "object.name='test-object-{{test}}'," +
                                "rotate.interval.ms=\"60000\", @map(type='text') )" +
                                "define stream outStream(key string, payload string, suffix string);",
                        description = "Above example demonstrate how an GCS sink is getting configured in order to " +
                                "publish messages to a GCS Bucket.\n" +
                                "Once an event is received by outStream, an text file will be generated by 'text' " +
                                "mapper from the attribute values of the event. GCS sink will connect to the " +
                                "bucket using provided configurations and upload the object to the bucket.\n"
                )
        }

)
public class GCSSink extends Sink<GCSSink.GCSSinkState> {
    private static final Logger logger = Logger.getLogger(GCSSink.class);

    private EventPublisher eventPublisher;
    private GCSSinkConfig gcsSinkConfig;

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     *
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[] {String.class, Event.class, ByteBuffer.class};
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[] {GCSConstants.OBJECT_NAME};
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param streamDefinition containing stream definition bind to the {@link Sink}
     * @param optionHolder     Option holder containing static and dynamic configuration related
     *                         to the {@link Sink}
     * @param configReader     to read the sink related system configuration.
     * @param siddhiAppContext the context of the {@link io.siddhi.query.api.SiddhiApp} used to
     *                         get siddhi related utility functions.
     * @return StateFactory for the Function which contains logic for the updated state based on arrived events.
     */
    @Override
    protected StateFactory<GCSSinkState> init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                              ConfigReader configReader,
                                              SiddhiAppContext siddhiAppContext) {
        this.gcsSinkConfig = new GCSSinkConfig(optionHolder, siddhiAppContext.getScheduledExecutorService());
        this.eventPublisher = new EventPublisher(gcsSinkConfig, optionHolder);

        return GCSSinkState::new;
    }


    /**
     * This method will be called when events need to be published via this sink
     *
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @param state          current state of the sink
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, GCSSinkState state)
            throws ConnectionUnavailableException {
        eventPublisher.publish(payload, dynamicOptions);
    }


    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        this.gcsSinkConfig.setMapType(getMapper().getType());
        eventPublisher.initializeServiceClient();
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {

    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {
        this.eventPublisher =  null;
    }

    /**
     * Returns the map type of a given Stream definition of a Sink
     * @param streamDefinition
     * @return
     */
    private String extractMapType(StreamDefinition streamDefinition) {
        Optional<Annotation> mapAnnotation = streamDefinition.getAnnotations()
                .stream()
                .filter(e -> e.getName().equals("sink"))
                .findFirst()
                .get()
                .getAnnotations()
                .stream()
                .filter(e -> e.getName().equals("map"))
                .findFirst();

        return mapAnnotation.map(annotation -> annotation.getElement("type"))
                                                .orElse(GCSConstants.DEFAULT_MAPPING_TYPE);
    }

    /**
     * Give information to the deployment about the service exposed by the sink.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    class GCSSinkState extends State {

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();

            try {
                if (eventPublisher.getStateContainer().lock()) {
                    state.put(GCSConstants.EVENT_OFFSET_MAP, eventPublisher.getStateContainer().getEventOffsetMap());
                    state.put(GCSConstants.EVENT_QUEUE_MAP, eventPublisher.getStateContainer().getQueuedEventMap());
                }
            } finally {
                eventPublisher.getStateContainer().releaseLock();
            }
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {

            try {
                if (eventPublisher.getStateContainer().lock()) {
                    eventPublisher.getStateContainer()
                            .setEventOffsetMap((HashMap<String, Integer>) state.get(GCSConstants.EVENT_OFFSET_MAP));
                    eventPublisher.getStateContainer()
                            .setEventOffsetMap((HashMap<String, Integer>) state.get(GCSConstants.EVENT_QUEUE_MAP));
                }
            } finally {
                eventPublisher.getStateContainer().releaseLock();
            }
        }
    }




}
