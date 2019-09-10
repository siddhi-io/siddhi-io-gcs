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

public class GCSConstants {

    // attribute names
    public static final String BUCKET_NAME =  "bucket.name";
    public static final String CREDENTIAL_FILE_PATH = "credential.provider.file.path";
    public static final String ENABLE_VERSIONING = "versioning.enabled";
    public static final String STORAGE_CLASS = "storage.class";
    public static final String CONTENT_TYPE = "content.type";
    public static final String BUCKET_ACL = "bucket.acl";
    public static final String OBJECT_ACL = "object.acl";
    public static final String OBJECT_METADATA = "object.metadata";
    public static final String OBJECT_NAME = "object.name";
    public static final String FLUSH_SIZE = "flush.size";
    public static final String ROTATE_INTERVAL = "rotate.interval.ms";
    public static final String ROTATE_SCHEDULED_INTERVAL = "rotate.scheduled.interval.ms";


    private GCSConstants() {
        // To prevent the initialization of class.
    }
}
