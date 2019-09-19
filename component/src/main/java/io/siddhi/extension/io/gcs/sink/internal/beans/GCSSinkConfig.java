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
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.gcs.util.GCSConfig;
import io.siddhi.extension.io.gcs.util.GCSConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
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
    private int flushSize = GCSConstants.DEFAULT_FLUSH_SIZE;
    private long rotateInterval = GCSConstants.DEFAULT_SPAN_INTERVAL;
    private int scheduledInterval = GCSConstants.DEFAULT_SCHEDULED_INTERVAL;
    private ScheduledExecutorService scheduledExecutorService;
    private String enclosingElement = GCSConstants.DEFAULT_ENCLOSING_ELEMENT;
    private String textDelimiter = GCSConstants.DEFAULT_TEXT_DELIMITER;
    private int flushTimeout;

    public GCSSinkConfig(OptionHolder optionHolder, ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
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
                    this.storageClass = StorageClass.valueOf(
                            optionHolder.validateAndGetStaticValue(GCSConstants.STORAGE_CLASS));
                    break;
                case GCSConstants.ENABLE_VERSIONING:
                    this.versioningEnabled = Boolean.parseBoolean(
                            optionHolder.validateAndGetStaticValue(GCSConstants.ENABLE_VERSIONING));
                    break;
                case GCSConstants.FLUSH_SIZE:
                    this.flushSize = Integer.parseInt(optionHolder
                            .validateAndGetStaticValue(GCSConstants.FLUSH_SIZE,
                                    String.valueOf(GCSConstants.DEFAULT_FLUSH_SIZE)));
                    break;
                case GCSConstants.ROTATE_INTERVAL_MS:
                    this.rotateInterval = Long
                            .parseLong(optionHolder
                                    .validateAndGetStaticValue(GCSConstants.ROTATE_INTERVAL_MS,
                                            String.valueOf(GCSConstants.DEFAULT_SPAN_INTERVAL)));
                    break;
                case GCSConstants.ROTATE_SCHEDULED_INTERVAL:
                    this.scheduledInterval = Integer.parseInt(
                            optionHolder.validateAndGetStaticValue(GCSConstants.ROTATE_SCHEDULED_INTERVAL,
                                    String.valueOf(GCSConstants.DEFAULT_SCHEDULED_INTERVAL)));
                    break;
                case GCSConstants.ENCLOSING_ELEMENT:
                    this.enclosingElement =
                            optionHolder.validateAndGetStaticValue(GCSConstants.ENCLOSING_ELEMENT,
                                    GCSConstants.DEFAULT_ENCLOSING_ELEMENT);
                    break;
                case GCSConstants.TEXT_DELIMITER:
                    this.textDelimiter = optionHolder
                            .validateAndGetStaticValue(GCSConstants.TEXT_DELIMITER,
                                                                            GCSConstants.DEFAULT_TEXT_DELIMITER);
                    break;
                case GCSConstants.FLUSH_TIMEOUT:
                    this.flushTimeout = Integer.parseInt(optionHolder
                            .validateAndGetStaticValue(GCSConstants.FLUSH_TIMEOUT, GCSConstants.DEFAULT_FLUSH_TIMEOUT));
                    break;
                default:
                    // Throw error?
            }
        });
    }

    private void setBucketACLMap(String bucketAclString) {
        Matcher matcher = Pattern.compile("[a-zA-Z.@0-9]+:[a-zA-Z]+").matcher(bucketAclString);

        if (matcher.find()) {
            do {
                this.bucketACLMap.put(matcher.group().split(":")[0], matcher.group().split(":")[1]);
            } while (matcher.find());
        }
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public int getFlushTimeout() {
        return flushTimeout;
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

    public long getRotateInterval() {
        return rotateInterval;
    }

    public int getScheduledInterval() {
        return scheduledInterval;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public String getEnclosingElement() {
        return enclosingElement;
    }

    public String getTextDelimiter() {
        return textDelimiter;
    }

    public String getFileType() {
        switch (mapType.toLowerCase()) {
            case "xml":
                return "xml";
            case "json":
                return "json";
            case "text":
                return "txt";
            case "avro":
                return "bin";
            case "binary":
                return "bin";
            default:
                return "bin";
        }
    }
}
