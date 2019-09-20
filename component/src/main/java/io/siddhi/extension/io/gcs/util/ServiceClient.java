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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.log4j.Logger;

public class ServiceClient {
    private Storage storage;
    private GCSConfig config;

    private static final Logger logger = Logger.getLogger(ServiceClient.class);

    public ServiceClient(GCSSinkConfig config) {
        this.config = config;
    }

    public ServiceClient initializeClient() throws ConnectionUnavailableException {
        try {
            if(config.getCredentialFilePath().isEmpty()) {
               storage = StorageOptions.getDefaultInstance().getService();
            } else {
               storage = StorageOptions.newBuilder()
                        .setCredentials(GoogleCredentials
                                .fromStream(new FileInputStream(
                                        new File(config.getCredentialFilePath())))).build().getService();
            }
        } catch (StorageException e) {
            throw new ConnectionUnavailableException(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return this;
    }

    /**
     * Sets Access Control Lists for GCS bucket given the user's email and role.
     *
     * @param email
     * @param role
     * @return
     */
    private Acl createAclForUser(String email, Acl.Role role) {
        return storage.createAcl(config.getBucketName(), Acl.of(new Acl.User(email), role));
    }

    /**
     * This method checks if the bucket exists and creates the bucket if it doesn't exist.
     */
    public ServiceClient createBucketIfNotExists() {
        // Check if the bucket exists in the GCS
        if (storage != null && storage.get(config.getBucketName(), Storage.BucketGetOption.fields()) == null) {
            //Create a bucket when it is not existing
            storage.create(BucketInfo.newBuilder(config.getBucketName())
                    .setStorageClass(((GCSSinkConfig) config).getStorageClass())
                    .setVersioningEnabled(((GCSSinkConfig) config).isEnableVersioning()).build());

            // Set user defined ACLs for the bucket
            for (Map.Entry<String, String> item : ((GCSSinkConfig) config).getBucketAcl().entrySet()) {
                switch (item.getValue().toLowerCase()) {
                    case "owner":
                        createAclForUser(item.getKey(), Acl.Role.OWNER);
                        break;
                    case "reader":
                        createAclForUser(item.getKey(), Acl.Role.READER);
                        break;
                    case "writer":
                        createAclForUser(item.getKey(), Acl.Role.WRITER);
                        break;
                    default:
                        // not a valid type of Permission.
                }
            }
        }

        return this;
    }

    public void uploadObject(String objectName, String objectContent) {
        BlobId blobId = BlobId.of(config.getBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType(getContentType(((GCSSinkConfig) config).getMapType()))
                .build();

        try {
            storage.create(blobInfo, objectContent.getBytes(StandardCharsets.UTF_8));
        } catch (StorageException e) {
            logger.error("Error while uploading object to GCS bucket", e);
        }
    }

    /**
     * Returns the content type of the file.
     *
     * @param mapType
     * @return
     */
    private String getContentType(String mapType) {
        switch (mapType.toLowerCase()) {
            case "json":
                return GCSConstants.JSON_CONTENT_TYPE;
            case "xml":
                return GCSConstants.XML_CONTENT_TYPE;
            case "text":
                return GCSConstants.TEXT_CONTENT_TYPE;
            case "avro":
                return GCSConstants.BINARY_CONTENT_TYPE;
            case "binary":
                return GCSConstants.BINARY_CONTENT_TYPE;
            default:
                return null;
        }
    }
}
