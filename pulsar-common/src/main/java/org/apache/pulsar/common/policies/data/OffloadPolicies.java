/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.policies.data;

import static org.apache.pulsar.common.util.FieldParser.value;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Definition of the offload policies.
 */
@Slf4j
@Data
public class OffloadPolicies implements Serializable {

    private final static long serialVersionUID = 0L;

    private final static List<Field> CONFIGURATION_FIELDS;
    static {
        CONFIGURATION_FIELDS = new ArrayList<>();
        Class<OffloadPolicies> clazz = OffloadPolicies.class;
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Configuration.class)) {
                CONFIGURATION_FIELDS.add(field);
            }
        }
    }

    public final static int DEFAULT_MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024 * 1024;   // 64MB
    public final static int DEFAULT_READ_BUFFER_SIZE_IN_BYTES = 1024 * 1024;      // 1MB
    public final static int DEFAULT_OFFLOAD_MAX_THREADS = 2;
    public final static int DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS = 1;
    public final static String[] DRIVER_NAMES = {"S3", "aws-s3", "google-cloud-storage", "filesystem"};
    public final static String DEFAULT_OFFLOADER_DIRECTORY = "./offloaders";
    public final static Long DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES = null;
    public final static Long DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS = null;

    public final static String OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE =
            "managedLedgerOffloadAutoTriggerSizeThresholdBytes";
    public final static String DELETION_LAG_NAME_IN_CONF_FILE = "managedLedgerOffloadDeletionLagMs";

    // common config
    @Configuration
    private String offloadersDirectory = DEFAULT_OFFLOADER_DIRECTORY;
    @Configuration
    private String managedLedgerOffloadDriver = null;
    @Configuration
    private Integer managedLedgerOffloadMaxThreads = DEFAULT_OFFLOAD_MAX_THREADS;
    @Configuration
    private Integer managedLedgerOffloadPrefetchRounds = DEFAULT_OFFLOAD_MAX_PREFETCH_ROUNDS;
    @Configuration
    private Long managedLedgerOffloadThresholdInBytes = DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES;
    @Configuration
    private Long managedLedgerOffloadDeletionLagInMillis = DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS;

    // s3 config, set by service configuration or cli
    @Configuration
    private String s3ManagedLedgerOffloadRegion = null;
    @Configuration
    private String s3ManagedLedgerOffloadBucket = null;
    @Configuration
    private String s3ManagedLedgerOffloadServiceEndpoint = null;
    @Configuration
    private Integer s3ManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    @Configuration
    private Integer s3ManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // s3 config, set by service configuration
    @Configuration
    private String s3ManagedLedgerOffloadRole = null;
    @Configuration
    private String s3ManagedLedgerOffloadRoleSessionName = "pulsar-s3-offload";

    // gcs config, set by service configuration or cli
    @Configuration
    private String gcsManagedLedgerOffloadRegion = null;
    @Configuration
    private String gcsManagedLedgerOffloadBucket = null;
    @Configuration
    private Integer gcsManagedLedgerOffloadMaxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
    @Configuration
    private Integer gcsManagedLedgerOffloadReadBufferSizeInBytes = DEFAULT_READ_BUFFER_SIZE_IN_BYTES;
    // gcs config, set by service configuration
    @Configuration
    private String gcsManagedLedgerOffloadServiceAccountKeyFile = null;

    // file system config, set by service configuration
    @Configuration
    private String fileSystemProfilePath = null;
    @Configuration
    private String fileSystemURI = null;

    public static OffloadPolicies create(String driver, String region, String bucket, String endpoint,
                                         Integer maxBlockSizeInBytes, Integer readBufferSizeInBytes,
                                         Long offloadThresholdInBytes, Long offloadDeletionLagInMillis) {
        OffloadPolicies offloadPolicies = new OffloadPolicies();
        offloadPolicies.setManagedLedgerOffloadDriver(driver);
        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(offloadThresholdInBytes);
        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(offloadDeletionLagInMillis);

        if (driver.equalsIgnoreCase(DRIVER_NAMES[0]) || driver.equalsIgnoreCase(DRIVER_NAMES[1])) {
            offloadPolicies.setS3ManagedLedgerOffloadRegion(region);
            offloadPolicies.setS3ManagedLedgerOffloadBucket(bucket);
            offloadPolicies.setS3ManagedLedgerOffloadServiceEndpoint(endpoint);
            offloadPolicies.setS3ManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes);
            offloadPolicies.setS3ManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        } else if (driver.equalsIgnoreCase(DRIVER_NAMES[2])) {
            offloadPolicies.setGcsManagedLedgerOffloadRegion(region);
            offloadPolicies.setGcsManagedLedgerOffloadBucket(bucket);
            offloadPolicies.setGcsManagedLedgerOffloadMaxBlockSizeInBytes(maxBlockSizeInBytes);
            offloadPolicies.setGcsManagedLedgerOffloadReadBufferSizeInBytes(readBufferSizeInBytes);
        }
        return offloadPolicies;
    }

    public static OffloadPolicies create(Properties properties) {
        OffloadPolicies data = new OffloadPolicies();
        Field[] fields = OffloadPolicies.class.getDeclaredFields();
        Arrays.stream(fields).forEach(f -> {
            if (properties.containsKey(f.getName())) {
                try {
                    f.setAccessible(true);
                    f.set(data, value((String) properties.get(f.getName()), f));
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("failed to initialize %s field while setting value %s",
                            f.getName(), properties.get(f.getName())), e);
                }
            }
        });
        data.compatibleWithBrokerConfigFile(properties);
        return data;
    }

    private void compatibleWithBrokerConfigFile(Properties properties) {
        if (!properties.containsKey("managedLedgerOffloadThresholdInBytes")
                && properties.containsKey(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE)) {
            setManagedLedgerOffloadThresholdInBytes(
                    Long.parseLong(properties.getProperty(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE)));
        }

        if (!properties.containsKey("managedLedgerOffloadDeletionLagInMillis")
                && properties.containsKey(DELETION_LAG_NAME_IN_CONF_FILE)) {
            setManagedLedgerOffloadDeletionLagInMillis(
                    Long.parseLong(properties.getProperty(DELETION_LAG_NAME_IN_CONF_FILE)));
        }
    }

    public boolean driverSupported() {
        return Arrays.stream(DRIVER_NAMES).anyMatch(d -> d.equalsIgnoreCase(this.managedLedgerOffloadDriver));
    }

    public static String getSupportedDriverNames() {
        return StringUtils.join(DRIVER_NAMES, ",");
    }

    public boolean isS3Driver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[0])
                || managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[1]);
    }

    public boolean isGcsDriver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[2]);
    }

    public boolean isFileSystemDriver() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        return managedLedgerOffloadDriver.equalsIgnoreCase(DRIVER_NAMES[3]);
    }

    public boolean bucketValid() {
        if (managedLedgerOffloadDriver == null) {
            return false;
        }
        if (isS3Driver()) {
            return StringUtils.isNotEmpty(s3ManagedLedgerOffloadBucket);
        } else if (isGcsDriver()) {
            return StringUtils.isNotEmpty(gcsManagedLedgerOffloadBucket);
        } else if (isFileSystemDriver()) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                managedLedgerOffloadDriver,
                managedLedgerOffloadMaxThreads,
                managedLedgerOffloadPrefetchRounds,
                managedLedgerOffloadThresholdInBytes,
                managedLedgerOffloadDeletionLagInMillis,
                s3ManagedLedgerOffloadRegion,
                s3ManagedLedgerOffloadBucket,
                s3ManagedLedgerOffloadServiceEndpoint,
                s3ManagedLedgerOffloadMaxBlockSizeInBytes,
                s3ManagedLedgerOffloadReadBufferSizeInBytes,
                s3ManagedLedgerOffloadRole,
                s3ManagedLedgerOffloadRoleSessionName,
                gcsManagedLedgerOffloadRegion,
                gcsManagedLedgerOffloadBucket,
                gcsManagedLedgerOffloadMaxBlockSizeInBytes,
                gcsManagedLedgerOffloadReadBufferSizeInBytes,
                gcsManagedLedgerOffloadServiceAccountKeyFile,
                fileSystemProfilePath,
                fileSystemURI);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OffloadPolicies other = (OffloadPolicies) obj;
        return Objects.equals(managedLedgerOffloadDriver, other.getManagedLedgerOffloadDriver())
                && Objects.equals(managedLedgerOffloadMaxThreads, other.getManagedLedgerOffloadMaxThreads())
                && Objects.equals(managedLedgerOffloadPrefetchRounds, other.getManagedLedgerOffloadPrefetchRounds())
                && Objects.equals(managedLedgerOffloadThresholdInBytes,
                    other.getManagedLedgerOffloadThresholdInBytes())
                && Objects.equals(managedLedgerOffloadDeletionLagInMillis,
                    other.getManagedLedgerOffloadDeletionLagInMillis())
                && Objects.equals(s3ManagedLedgerOffloadRegion, other.getS3ManagedLedgerOffloadRegion())
                && Objects.equals(s3ManagedLedgerOffloadBucket, other.getS3ManagedLedgerOffloadBucket())
                && Objects.equals(s3ManagedLedgerOffloadServiceEndpoint,
                    other.getS3ManagedLedgerOffloadServiceEndpoint())
                && Objects.equals(s3ManagedLedgerOffloadMaxBlockSizeInBytes,
                    other.getS3ManagedLedgerOffloadMaxBlockSizeInBytes())
                && Objects.equals(s3ManagedLedgerOffloadReadBufferSizeInBytes,
                    other.getS3ManagedLedgerOffloadReadBufferSizeInBytes())
                && Objects.equals(s3ManagedLedgerOffloadRole, other.getS3ManagedLedgerOffloadRole())
                && Objects.equals(s3ManagedLedgerOffloadRoleSessionName,
                    other.getS3ManagedLedgerOffloadRoleSessionName())
                && Objects.equals(gcsManagedLedgerOffloadRegion, other.getGcsManagedLedgerOffloadRegion())
                && Objects.equals(gcsManagedLedgerOffloadBucket, other.getGcsManagedLedgerOffloadBucket())
                && Objects.equals(gcsManagedLedgerOffloadMaxBlockSizeInBytes,
                    other.getGcsManagedLedgerOffloadMaxBlockSizeInBytes())
                && Objects.equals(gcsManagedLedgerOffloadReadBufferSizeInBytes,
                    other.getGcsManagedLedgerOffloadReadBufferSizeInBytes())
                && Objects.equals(gcsManagedLedgerOffloadServiceAccountKeyFile,
                    other.getGcsManagedLedgerOffloadServiceAccountKeyFile())
                && Objects.equals(fileSystemProfilePath, other.getFileSystemProfilePath())
                && Objects.equals(fileSystemURI, other.getFileSystemURI());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("managedLedgerOffloadDriver", managedLedgerOffloadDriver)
                .add("managedLedgerOffloadMaxThreads", managedLedgerOffloadMaxThreads)
                .add("managedLedgerOffloadPrefetchRounds", managedLedgerOffloadPrefetchRounds)
                .add("managedLedgerOffloadAutoTriggerSizeThresholdBytes",
                        managedLedgerOffloadThresholdInBytes)
                .add("managedLedgerOffloadDeletionLagInMillis", managedLedgerOffloadDeletionLagInMillis)
                .add("s3ManagedLedgerOffloadRegion", s3ManagedLedgerOffloadRegion)
                .add("s3ManagedLedgerOffloadBucket", s3ManagedLedgerOffloadBucket)
                .add("s3ManagedLedgerOffloadServiceEndpoint", s3ManagedLedgerOffloadServiceEndpoint)
                .add("s3ManagedLedgerOffloadMaxBlockSizeInBytes", s3ManagedLedgerOffloadMaxBlockSizeInBytes)
                .add("s3ManagedLedgerOffloadReadBufferSizeInBytes", s3ManagedLedgerOffloadReadBufferSizeInBytes)
                .add("s3ManagedLedgerOffloadRole", s3ManagedLedgerOffloadRole)
                .add("s3ManagedLedgerOffloadRoleSessionName", s3ManagedLedgerOffloadRoleSessionName)
                .add("gcsManagedLedgerOffloadRegion", gcsManagedLedgerOffloadRegion)
                .add("gcsManagedLedgerOffloadBucket", gcsManagedLedgerOffloadBucket)
                .add("gcsManagedLedgerOffloadMaxBlockSizeInBytes", gcsManagedLedgerOffloadMaxBlockSizeInBytes)
                .add("gcsManagedLedgerOffloadReadBufferSizeInBytes", gcsManagedLedgerOffloadReadBufferSizeInBytes)
                .add("gcsManagedLedgerOffloadServiceAccountKeyFile", gcsManagedLedgerOffloadServiceAccountKeyFile)
                .add("fileSystemProfilePath", fileSystemProfilePath)
                .add("fileSystemURI", fileSystemURI)
                .toString();
    }

    public static final String METADATA_FIELD_BUCKET = "bucket";
    public static final String METADATA_FIELD_REGION = "region";
    public static final String METADATA_FIELD_ENDPOINT = "serviceEndpoint";
    public static final String METADATA_FIELD_MAX_BLOCK_SIZE = "maxBlockSizeInBytes";
    public static final String METADATA_FIELD_READ_BUFFER_SIZE = "readBufferSizeInBytes";
    public static final String OFFLOADER_PROPERTY_PREFIX = "managedLedgerOffload.";

    public Properties toProperties() {
        Properties properties = new Properties();

        setProperty(properties, "offloadersDirectory", this.getOffloadersDirectory());
        setProperty(properties, "managedLedgerOffloadDriver", this.getManagedLedgerOffloadDriver());
        setProperty(properties, "managedLedgerOffloadMaxThreads",
                this.getManagedLedgerOffloadMaxThreads());
        setProperty(properties, "managedLedgerOffloadPrefetchRounds",
                this.getManagedLedgerOffloadPrefetchRounds());
        setProperty(properties, "managedLedgerOffloadThresholdInBytes",
                this.getManagedLedgerOffloadThresholdInBytes());
        setProperty(properties, "managedLedgerOffloadDeletionLagInMillis",
                this.getManagedLedgerOffloadDeletionLagInMillis());

        if (this.isS3Driver()) {
            setProperty(properties, "s3ManagedLedgerOffloadRegion",
                    this.getS3ManagedLedgerOffloadRegion());
            setProperty(properties, "s3ManagedLedgerOffloadBucket",
                    this.getS3ManagedLedgerOffloadBucket());
            setProperty(properties, "s3ManagedLedgerOffloadServiceEndpoint",
                    this.getS3ManagedLedgerOffloadServiceEndpoint());
            setProperty(properties, "s3ManagedLedgerOffloadMaxBlockSizeInBytes",
                    this.getS3ManagedLedgerOffloadMaxBlockSizeInBytes());
            setProperty(properties, "s3ManagedLedgerOffloadRole",
                    this.getS3ManagedLedgerOffloadRole());
            setProperty(properties, "s3ManagedLedgerOffloadRoleSessionName",
                    this.getS3ManagedLedgerOffloadRoleSessionName());
            setProperty(properties, "s3ManagedLedgerOffloadReadBufferSizeInBytes",
                    this.getS3ManagedLedgerOffloadReadBufferSizeInBytes());
        } else if (this.isGcsDriver()) {
            setProperty(properties, "gcsManagedLedgerOffloadRegion",
                    this.getGcsManagedLedgerOffloadRegion());
            setProperty(properties, "gcsManagedLedgerOffloadBucket",
                    this.getGcsManagedLedgerOffloadBucket());
            setProperty(properties, "gcsManagedLedgerOffloadMaxBlockSizeInBytes",
                    this.getGcsManagedLedgerOffloadMaxBlockSizeInBytes());
            setProperty(properties, "gcsManagedLedgerOffloadReadBufferSizeInBytes",
                    this.getGcsManagedLedgerOffloadReadBufferSizeInBytes());
            setProperty(properties, "gcsManagedLedgerOffloadServiceAccountKeyFile",
                    this.getGcsManagedLedgerOffloadServiceAccountKeyFile());
        } else if (this.isFileSystemDriver()) {
            setProperty(properties, "fileSystemProfilePath", this.getFileSystemProfilePath());
            setProperty(properties, "fileSystemURI", this.getFileSystemURI());
        }
        return properties;
    }

    private static void setProperty(Properties properties, String key, Object value) {
        if (value != null) {
            properties.setProperty(key, "" + value);
        }
    }

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface Configuration {

    }

    /**
     * This method is used to make a compatible with old policies.
     *
     * The filed {@link Policies#offload_threshold} is primitive, so it can't be known whether it had been set.
     * In the old logic, if the field value is -1, it could be thought that the field had not been set.
     *
     * @param nsLevelPolicies  namespace level offload policies
     * @param policies namespace policies
     */
    public static void oldPoliciesCompatible(OffloadPolicies nsLevelPolicies, Policies policies) {
        if (nsLevelPolicies == null || policies == null) {
            return;
        }
        if (nsLevelPolicies.getManagedLedgerOffloadThresholdInBytes() == null
                && policies.offload_threshold != -1) {
            nsLevelPolicies.setManagedLedgerOffloadThresholdInBytes(policies.offload_threshold);
        }
        if (nsLevelPolicies.getManagedLedgerOffloadDeletionLagInMillis() == null
                && policies.offload_deletion_lag_ms != null) {
            nsLevelPolicies.setManagedLedgerOffloadDeletionLagInMillis(policies.offload_deletion_lag_ms);
        }
    }

    /**
     * Merge different level offload policies.
     *
     * policies level priority: topic > namespace > broker
     *
     * @param topicLevelPolicies topic level offload policies
     * @param nsLevelPolicies namesapce level offload policies
     * @param brokerProperties broker level offload configuration
     *
     * @return offload policies
     */
    public static OffloadPolicies mergeConfiguration(OffloadPolicies topicLevelPolicies,
                                           OffloadPolicies nsLevelPolicies,
                                           Properties brokerProperties) {
        try {
            boolean allConfigValuesAreNull = true;
            OffloadPolicies offloadPolicies = new OffloadPolicies();
            for (Field field : CONFIGURATION_FIELDS) {
                Object object;
                if (topicLevelPolicies != null && field.get(topicLevelPolicies) != null) {
                    object = field.get(topicLevelPolicies);
                } else if (nsLevelPolicies != null && field.get(nsLevelPolicies) != null) {
                    object = field.get(nsLevelPolicies);
                } else {
                    object = getCompatibleValue(brokerProperties, field);
                }
                if (object != null) {
                    field.set(offloadPolicies, object);
                    if (allConfigValuesAreNull) {
                        allConfigValuesAreNull = false;
                    }
                }
            }
            if (allConfigValuesAreNull) {
                return null;
            } else {
                return offloadPolicies;
            }
        } catch (Exception e) {
            log.error("Failed to merge configuration.", e);
            return null;
        }
    }

    /**
     * Make configurations of the OffloadPolicies compatible with the config file.
     *
     * The names of the fields {@link OffloadPolicies#managedLedgerOffloadDeletionLagInMillis}
     * and {@link OffloadPolicies#managedLedgerOffloadThresholdInBytes} are not matched with
     * config file (broker.conf or standalone.conf).
     *
     * @param properties
     * @param field
     * @return
     */
    private static Object getCompatibleValue(Properties properties, Field field) {
        Object object = null;
        if (field.getName().equals("managedLedgerOffloadThresholdInBytes")) {
            object = properties.getProperty("managedLedgerOffloadThresholdInBytes",
                    properties.getProperty(OFFLOAD_THRESHOLD_NAME_IN_CONF_FILE));
        } else if (field.getName().equals("")) {
            object = properties.getProperty("managedLedgerOffloadDeletionLagInMillis",
                    properties.getProperty(DELETION_LAG_NAME_IN_CONF_FILE));
        } else {
            object = properties.get(field.getName());
        }
        return value((String) object, field);
    }

}
