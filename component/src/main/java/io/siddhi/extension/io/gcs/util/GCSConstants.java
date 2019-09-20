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

package io.siddhi.extension.io.gcs.util;

/**
 * Class to define constants that are required for the GCS Source/Sink.
 */
public class GCSConstants {

    // attribute names
    public static final String BUCKET_NAME = "bucket.name";
    public static final String CREDENTIAL_FILE_PATH = "credential.path";
    public static final String ENABLE_VERSIONING = "versioning.enabled";
    public static final String STORAGE_CLASS = "storage.class";
    public static final String CONTENT_TYPE = "content.type";
    public static final String BUCKET_ACL = "bucket.acl";
    public static final String OBJECT_NAME = "object.name";
    public static final String FLUSH_SIZE = "flush.size";
    public static final String FLUSH_TIMEOUT = "flush.timeout";
    public static final String ROTATE_INTERVAL_MS = "rotate.interval.ms";
    public static final String ROTATE_SCHEDULED_INTERVAL = "rotate.scheduled.interval.ms";
    public static final String ENCLOSING_ELEMENT = "xml.enclosing.element";
    public static final String TEXT_DELIMITER = "text.delimiter";

    public static final String DEFAULT_MAPPING_TYPE = "passthrough";
    public static final String DEFAULT_ENCLOSING_ELEMENT = "Elements";

    // state variable keys
    public static final String EVENT_OFFSET_MAP = "event.offset.map";
    public static final String EVENT_QUEUE_MAP = "event.queue.map";

    // content-types
    public static final String TEXT_CONTENT_TYPE = "text/plain";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String XML_CONTENT_TYPE = "application/xml";
    public static final String BINARY_CONTENT_TYPE = "application/octet-stream";

    public static final int DEFAULT_FLUSH_SIZE = 1;
    public static final int DEFAULT_SPAN_INTERVAL = -1;
    public static final int DEFAULT_SCHEDULED_INTERVAL = -1;
    public static final String DEFAULT_FLUSH_TIMEOUT = "15000";
    public static final String DEFAULT_TEXT_DELIMITER = "~~~~~~~~~";

    private GCSConstants() {
        // To prevent the initialization of class.
    }
}
