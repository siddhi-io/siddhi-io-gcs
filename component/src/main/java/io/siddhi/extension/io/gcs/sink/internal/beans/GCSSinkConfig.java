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

/**
 * Class containing configs related to GCSSink.
 */
public class GCSSinkConfig extends GCSConfig {
    private boolean enableVersioning;
    private StorageClass storageClass;
    private String contentType;
    private Map<String, String> bucketAcl = new HashMap<>();
    private String mapType;

    public GCSSinkConfig(OptionHolder optionHolder) {
        optionHolder.getStaticOptionsKeys().forEach(key -> {
            switch (key.toLowerCase()) {
                case GCSConstants.BUCKET_NAME:
                    super.setBucketName(optionHolder.validateAndGetStaticValue(GCSConstants.BUCKET_NAME));
                    break;
                case GCSConstants.CREDENTIAL_FILE_PATH:
                    super.setCredentialFilePath(optionHolder
                            .validateAndGetStaticValue(GCSConstants.CREDENTIAL_FILE_PATH));
                    break;
                case GCSConstants.CONTENT_TYPE:
                    this.contentType = optionHolder.validateAndGetStaticValue(GCSConstants.CONTENT_TYPE,
                            GCSConstants.DEFAULT_CONTENT_TYPE);
                    break;
                case GCSConstants.STORAGE_CLASS:
                    this.storageClass = StorageClass.valueOf(optionHolder
                            .validateAndGetStaticValue(GCSConstants.STORAGE_CLASS, GCSConstants.DEFAULT_STORAGE_CLASS));
                    break;
                case GCSConstants.ENABLE_VERSIONING:
                    this.enableVersioning = Boolean.parseBoolean(
                           optionHolder.validateAndGetStaticValue(GCSConstants.ENABLE_VERSIONING,
                                   GCSConstants.DEFAULT_ENABLE_VERSIONING));
                    break;
                default:
                    // not a valid config attribute
            }
        });
    }

    public boolean isEnableVersioning() {
        return enableVersioning;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public String getContentType() {
        return contentType;
    }

    public Map<String, String> getBucketAcl() {
        return bucketAcl;
    }

    public String getMapType() {
        return mapType;
    }

    public void setMapType(String mapType) {
        this.mapType = mapType;
    }
}
