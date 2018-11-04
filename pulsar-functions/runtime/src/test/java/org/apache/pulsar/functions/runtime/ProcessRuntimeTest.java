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

package org.apache.pulsar.functions.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.util.JsonFormat;

import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kubernetes.client.models.V1PodSpec;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.ConsumerSpec;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link ThreadRuntime}.
 */
public class ProcessRuntimeTest {

    class TestSecretsProviderConfigurator implements SecretsProviderConfigurator {

        @Override
        public void init(Map<String, String> config) {

        }

        @Override
        public String getSecretsProviderClassName(FunctionDetails functionDetails) {
            if (functionDetails.getRuntime() == FunctionDetails.Runtime.JAVA) {
                return ClearTextSecretsProvider.class.getName();
            } else {
                return "secretsprovider.ClearTextSecretsProvider";
            }
        }

        @Override
        public Map<String, String> getSecretsProviderConfig(FunctionDetails functionDetails) {
            Map<String, String> config = new HashMap<>();
            config.put("Config", "Value");
            return config;
        }

        @Override
        public void configureKubernetesRuntimeSecretsProvider(V1PodSpec podSpec, String functionsContainerName, FunctionDetails functionDetails) {
        }

        @Override
        public void configureProcessRuntimeSecretsProvider(ProcessBuilder processBuilder, FunctionDetails functionDetails) {
        }

        @Override
        public Type getSecretObjectType() {
            return TypeToken.get(String.class).getType();
        }

        @Override
        public void validateSecretMap(Map<String, Object> secretMap) {

        }
    }

    private static final String TEST_TENANT = "test-function-tenant";
    private static final String TEST_NAMESPACE = "test-function-namespace";
    private static final String TEST_NAME = "test-function-container";
    private static final Map<String, String> topicsToSerDeClassName = new HashMap<>();
    private static final Map<String, ConsumerSpec> topicsToSchema = new HashMap<>();
    static {
        topicsToSerDeClassName.put("persistent://sample/standalone/ns1/test_src", "");
        topicsToSchema.put("persistent://sample/standalone/ns1/test_src",
                ConsumerSpec.newBuilder().setSerdeClassName("").setIsRegexPattern(false).build());
    }

    private ProcessRuntimeFactory factory;
    private final String userJarFile;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String pulsarServiceUrl;
    private final String stateStorageServiceUrl;
    private final String logDirectory;

    public ProcessRuntimeTest() {
        this.userJarFile = "/Users/user/UserJar.jar";
        this.javaInstanceJarFile = "/Users/user/JavaInstance.jar";
        this.pythonInstanceFile = "/Users/user/PythonInstance.py";
        this.pulsarServiceUrl = "pulsar://localhost:6670";
        this.stateStorageServiceUrl = "bk://localhost:4181";
        this.logDirectory = "Users/user/logs";
    }

    @AfterMethod
    public void tearDown() {
        if (null != this.factory) {
            this.factory.close();
        }
    }

    private ProcessRuntimeFactory createProcessRuntimeFactory(String extraDependenciesDir) {
        return new ProcessRuntimeFactory(
            pulsarServiceUrl,
            stateStorageServiceUrl,
            null, /* auth config */
            javaInstanceJarFile,
            pythonInstanceFile,
            logDirectory,
            extraDependenciesDir, /* extra dependencies dir */
            new TestSecretsProviderConfigurator());
    }

    FunctionDetails createFunctionDetails(FunctionDetails.Runtime runtime) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        functionDetailsBuilder.setRuntime(runtime);
        functionDetailsBuilder.setTenant(TEST_TENANT);
        functionDetailsBuilder.setNamespace(TEST_NAMESPACE);
        functionDetailsBuilder.setName(TEST_NAME);
        functionDetailsBuilder.setClassName("org.apache.pulsar.functions.utils.functioncache.AddFunction");
        functionDetailsBuilder.setSink(Function.SinkSpec.newBuilder()
                .setTopic(TEST_NAME + "-output")
                .setSerDeClassName("org.apache.pulsar.functions.runtime.serde.Utf8Serializer")
                .setClassName("org.pulsar.pulsar.TestSink")
                .setTypeClassName(String.class.getName())
                .build());
        functionDetailsBuilder.setLogTopic(TEST_NAME + "-log");
        functionDetailsBuilder.setSource(Function.SourceSpec.newBuilder()
                .setSubscriptionType(Function.SubscriptionType.FAILOVER)
                .putAllInputSpecs(topicsToSchema)
                .setClassName("org.pulsar.pulsar.TestSource")
                .setTypeClassName(String.class.getName()));
        return functionDetailsBuilder.build();
    }

    InstanceConfig createJavaInstanceConfig(FunctionDetails.Runtime runtime) {
        InstanceConfig config = new InstanceConfig();

        config.setFunctionDetails(createFunctionDetails(runtime));
        config.setFunctionId(java.util.UUID.randomUUID().toString());
        config.setFunctionVersion("1.0");
        config.setInstanceId(0);
        config.setMaxBufferedTuples(1024);

        return config;
    }

    @Test
    public void testJavaConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        factory = createProcessRuntimeFactory(null);

        verifyJavaInstance(config);
    }

    @Test
    public void testJavaConstructorWithEmptyExtraDepsDirString() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        factory = createProcessRuntimeFactory("");

        verifyJavaInstance(config);
    }

    @Test
    public void testJavaConstructorWithNoneExistentDir() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        factory = createProcessRuntimeFactory("/path/to/non-existent/dir");

        verifyJavaInstance(config, Paths.get("/path/to/non-existent/dir"));
    }

    @Test
    public void testJavaConstructorWithEmptyDir() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        Path dir = Files.createTempDirectory("test-empty-dir");
        assertTrue(Files.exists(dir));
        try {
            factory = createProcessRuntimeFactory(dir.toAbsolutePath().toString());

            verifyJavaInstance(config, dir);
        } finally {
            MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
        }
    }

    @Test
    public void testJavaConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.JAVA);

        Path dir = Files.createTempDirectory("test-empty-dir");
        assertTrue(Files.exists(dir));
        try {
            int numFiles = 3;
            for (int i = 0; i < numFiles; i++) {
                Path file = Files.createFile(Paths.get(dir.toAbsolutePath().toString(), "file-" + i));
                assertTrue(Files.exists(file));
            }

            factory = createProcessRuntimeFactory(dir.toAbsolutePath().toString());

            verifyJavaInstance(config, dir);
        } finally {
            MoreFiles.deleteRecursively(dir, RecursiveDeleteOption.ALLOW_INSECURE);
        }
    }

    private void verifyJavaInstance(InstanceConfig config) throws Exception {
        verifyJavaInstance(config, null);
    }

    private void verifyJavaInstance(InstanceConfig config, Path depsDir) throws Exception {
        ProcessRuntime container = factory.createContainer(config, userJarFile, null, 30l);
        List<String> args = container.getProcessArgs();

        String classpath = javaInstanceJarFile;
        String extraDepsEnv;
        int portArg;
        if (null != depsDir) {
            assertEquals(args.size(), 33);
            extraDepsEnv = " -Dpulsar.functions.extra.dependencies.dir=" + depsDir.toString();
            classpath = classpath + ":" + depsDir + "/*";
            portArg = 24;
        } else {
            assertEquals(args.size(), 32);
            extraDepsEnv = "";
            portArg = 23;
        }

        String expectedArgs = "java -cp " + classpath
                + " -Dpulsar.functions.java.instance.jar=" + javaInstanceJarFile
                + extraDepsEnv
                + " -Dlog4j.configurationFile=java_instance_log4j2.yml "
                + "-Dpulsar.function.log.dir=" + logDirectory + "/functions/" + FunctionDetailsUtils.getFullyQualifiedName(config.getFunctionDetails())
                + " -Dpulsar.function.log.file=" + config.getFunctionDetails().getName() + "-" + config.getInstanceId()
                + " org.apache.pulsar.functions.runtime.JavaInstanceMain"
                + " --jar " + userJarFile + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '" + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(portArg)
                + " --state_storage_serviceurl " + stateStorageServiceUrl
                + " --expected_healthcheck_interval 30"
                + " --secrets_provider org.apache.pulsar.functions.secretsprovider.ClearTextSecretsProvider"
                + " --secrets_provider_config {\"Config\":\"Value\"}";
        assertEquals(String.join(" ", args), expectedArgs);
    }

    @Test
    public void testPythonConstructor() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON);

        factory = createProcessRuntimeFactory(null);

        verifyPythonInstance(config, null);
    }

    @Test
    public void testPythonConstructorWithDeps() throws Exception {
        InstanceConfig config = createJavaInstanceConfig(FunctionDetails.Runtime.PYTHON);

        String extraDeps = "/path/to/extra/deps";

        factory = createProcessRuntimeFactory(extraDeps);

        verifyPythonInstance(config, extraDeps);
    }

    private void verifyPythonInstance(InstanceConfig config, String extraDepsDir) throws Exception {
        ProcessRuntime container = factory.createContainer(config, userJarFile, null, 30l);
        List<String> args = container.getProcessArgs();

        int totalArgs;
        int portArg;
        String pythonPath;
        int configArg;
        if (null == extraDepsDir) {
            totalArgs = 30;
            portArg = 23;
            configArg = 9;
            pythonPath = "";
        } else {
            totalArgs = 31;
            portArg = 24;
            configArg = 10;
            pythonPath = "PYTHONPATH=${PYTHONPATH}:" + extraDepsDir + " ";
        }

        assertEquals(args.size(), totalArgs);
        String expectedArgs = pythonPath + "python " + pythonInstanceFile
                + " --py " + userJarFile + " --logging_directory "
                + logDirectory + "/functions" + " --logging_file " + config.getFunctionDetails().getName()
                + " --logging_config_file " + args.get(configArg) + " --instance_id "
                + config.getInstanceId() + " --function_id " + config.getFunctionId()
                + " --function_version " + config.getFunctionVersion()
                + " --function_details '" + JsonFormat.printer().omittingInsignificantWhitespace().print(config.getFunctionDetails())
                + "' --pulsar_serviceurl " + pulsarServiceUrl
                + " --max_buffered_tuples 1024 --port " + args.get(portArg)
                + " --expected_healthcheck_interval 30"
                + " --secrets_provider secretsprovider.ClearTextSecretsProvider"
                + " --secrets_provider_config {\"Config\":\"Value\"}";
        assertEquals(String.join(" ", args), expectedArgs);
    }

}
