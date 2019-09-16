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

package io.siddhi.extension.io.gcs.sink.internal.beans;

import com.google.cloud.storage.StorageClass;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.util.GCSConfig;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to hold configurations required for the GCS sink.
 */
public class GCSSinkConfig extends GCSConfig {
    private StorageClass storageClass;
    private boolean versioningEnabled;
    private String contentType;
    private String mapType;
    private Map<String, String> bucketACLMap = new HashMap<>();
    private int flushSize;
    private int rotateInterval;
    private int scheduledInterval;


    public GCSSinkConfig(OptionHolder optionHolder) {
        optionHolder.getStaticOptionsKeys().forEach(key -> {
            switch (key.toLowerCase()) {
                case GCSConstants.BUCKET_NAME:
                    super.setBucketName(optionHolder.validateAndGetStaticValue(GCSConstants.BUCKET_NAME));
                    break;
                case GCSConstants.CREDENTIAL_FILE_PATH:
                    super.setAuthFilePath(optionHolder.validateAndGetStaticValue(GCSConstants.CREDENTIAL_FILE_PATH));
                    break;
                case GCSConstants.CONTENT_TYPE:
                    this.contentType = optionHolder.validateAndGetStaticValue(GCSConstants.CONTENT_TYPE);
                    break;
                case GCSConstants.BUCKET_ACL:
                    this.setBucketACLMap(optionHolder.validateAndGetStaticValue(GCSConstants.BUCKET_ACL));
                    break;
                case GCSConstants.STORAGE_CLASS:
                    this.storageClass = getStorageClassByName(
                            optionHolder.validateAndGetStaticValue(GCSConstants.STORAGE_CLASS));
                    break;
                case GCSConstants.ENABLE_VERSIONING:
                    this.versioningEnabled = Boolean.parseBoolean(
                            optionHolder.validateAndGetStaticValue(GCSConstants.ENABLE_VERSIONING));
                    break;
                case GCSConstants.FLUSH_SIZE:
                    this.flushSize = Integer.parseInt(optionHolder.validateAndGetStaticValue(GCSConstants.FLUSH_SIZE));
                    break;
                case GCSConstants.ROTATE_INTERVAL:
                    this.rotateInterval = Integer
                            .parseInt(optionHolder.validateAndGetStaticValue(GCSConstants.ROTATE_INTERVAL));
                    break;
                case GCSConstants.ROTATE_SCHEDULED_INTERVAL:
                    this.scheduledInterval = Integer.parseInt(
                            optionHolder.validateAndGetStaticValue(GCSConstants.ROTATE_SCHEDULED_INTERVAL));
                    break;
                default:
                    // Throw error?
            }
        });
    }

    private StorageClass getStorageClassByName(String storageClassName) {
        switch (storageClassName.toLowerCase()) {
            case "multi-regional":
                return StorageClass.MULTI_REGIONAL;
            case "regional":
                return StorageClass.REGIONAL;
            case "nearline":
                return StorageClass.NEARLINE;
            case "coldline":
                return StorageClass.COLDLINE;
            default:
                // not a supported version of StorageClass
                throw new SiddhiAppRuntimeException("Invalid Configuration provided for Storage class");
        }
    }

    private void setBucketACLMap(String bucketAclString) {
        Matcher matcher = Pattern.compile("[a-zA-Z.@0-9]+:[a-zA-Z]+").matcher(bucketAclString);

        while (matcher.find()) {
            this.bucketACLMap.put(matcher.group().split(":")[0], matcher.group().split(":")[1]);
        }
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public String getContentType() {
        return contentType;
    }

    public Map<String, String> getBucketACLMap() {
        return bucketACLMap;
    }

    public String getMapType() {
        return mapType;
    }

    public void setMapType(String mapType) {
        this.mapType = mapType;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public int getRotateInterval() {
        return rotateInterval;
    }

    public int getScheduledInterval() {
        return scheduledInterval;
    }
}
