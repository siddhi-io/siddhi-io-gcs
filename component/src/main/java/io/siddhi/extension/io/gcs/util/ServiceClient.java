package io.siddhi.extension.io.gcs.util;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.siddhi.extension.io.gcs.sink.internal.beans.GCSSinkConfig;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Contains logic required to publish and consume messages from GCS.
 */
public class ServiceClient {
    private GCSConfig config;
    private Storage storage;

    private Logger logger = Logger.getLogger(ServiceClient.class);

    public ServiceClient(GCSConfig config) {
        this.config = config;
        this.storage = initializeGCSCLient();
        this.createBucketIfNotExists();
    }

    /**
     * Initializes and returns the GCS client
     *
     * @return
     */
    private Storage initializeGCSCLient() {
        Storage gcsClient = null;

        if (config.getAuthFilePath() != null) {
            // Initialize the GCS client with the user authentication.
            try {
                gcsClient = StorageOptions.newBuilder()
                        .setCredentials(GoogleCredentials
                                .fromStream(new FileInputStream(
                                        new File(config.getAuthFilePath())))).build().getService();
            } catch (IOException e) {
                logger.error("Authentication with Google Cloud Storage failed please " +
                        "check the Authorization credentials again", e);
            }
        } else {
            gcsClient = StorageOptions.getDefaultInstance().getService();
        }

        return gcsClient;
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
     * This method checks if the bucket exists and creates the bucket if it doesn't exist
     */
    private void createBucketIfNotExists() {
        // Check if the bucket exists in the GCS

        if (storage != null && storage.get(config.getBucketName(), Storage.BucketGetOption.fields()) == null) {

            //Create a bucket when it is not existing
            storage.create(BucketInfo.newBuilder(config.getBucketName())
                    .setStorageClass(((GCSSinkConfig) config).getStorageClass())
                    .setVersioningEnabled(((GCSSinkConfig) config).isVersioningEnabled()).build());

            // Set user defined ACLs for the bucket
            for (Map.Entry<String, String> item : ((GCSSinkConfig) config).getBucketACLMap().entrySet()) {
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
    }

    /**
     * Logic to handle uploading of Objects
     */
    public void uploadObject(String objectName, String objectContent) {
        BlobId blobId = BlobId.of(config.getBucketName(), objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType(getContentType(((GCSSinkConfig) config).getMapType()))
                .build();

        try {
            storage.create(blobInfo, objectContent.getBytes(StandardCharsets.UTF_8));
        } catch (NullPointerException e) {
            logger.error("Error", e);
        }
    }

    /**
     * Returns the content type of the file
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
