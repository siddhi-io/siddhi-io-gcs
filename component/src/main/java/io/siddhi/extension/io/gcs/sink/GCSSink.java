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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import io.siddhi.query.api.definition.StreamDefinition;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

@Extension(
        name = "google-cloud-storage",
        namespace = "sink",
        description = " ",
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
                        optional = true
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
                        defaultValue = "null",
                        description = "Access Control List for the bucket level ACL defined as a key value pair list" +
                                " defined as \"'<key>:<value>','<key>:<value>'\""
                ),
                @Parameter(
                        name = GCSConstants.OBJECT_ACL,
                        type = DataType.STRING,
                        optional = true,
                        dynamic = true,
                        description = "Access Control List for the object level ACL defined as a key value pair list" +
                                " defined as \"'<key>:<value>','<key>:<value>'\""
                ),
                @Parameter(
                        name = GCSConstants.OBJECT_METADATA,
                        type = DataType.STRING,
                        optional = true,
                        dynamic = true,
                        description = "Object level metadata for the object defined as a key value pair list" +
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
                        name = GCSConstants.ROTATE_INTERVAL,
                        type = DataType.INT,
                        optional = true,
                        defaultValue = "-1",
                        description = "Maximum span of event time"
                ),
                @Parameter(
                        name = GCSConstants.ROTATE_SCHEDULED_INTERVAL,
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "-1",
                        description = "Maximum span of event time from the first event"
                ),
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)

// for more information refer https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/#sink

public class GCSSink extends Sink {

    private GCSSinkConfig gcsSinkConfig;
    private OptionHolder optionHolder;
    private Storage storage;
    private String mapType;

    private static final Logger logger = Logger.getLogger(GCSSink.class);


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
        return new Class[] {String.class, ByteBuffer.class};
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[] {GCSConstants.OBJECT_ACL, GCSConstants.OBJECT_NAME};
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
    protected StateFactory init(StreamDefinition streamDefinition, OptionHolder optionHolder, ConfigReader configReader,
                                SiddhiAppContext siddhiAppContext) {
        this.gcsSinkConfig = new GCSSinkConfig(optionHolder);
        this.optionHolder = optionHolder;

        try {
            // Initialize the GCS client with the user authentication.
            storage = StorageOptions.newBuilder()
                    .setCredentials(GoogleCredentials
                            .fromStream(new FileInputStream(
                                    new File(gcsSinkConfig.getAuthFilePath())))).build().getService();

        } catch (IOException e) {
            logger.error("Authentication with Google Cloud Storage failed please " +
                    "check the Authorization credentials again", e);
        }

        return null;
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
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {

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

}
