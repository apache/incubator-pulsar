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
package org.apache.pulsar.io.flume.node;

import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

public final class TestEnvVarResolverProperties {
    private static final File TESTFILE = new File(
            TestEnvVarResolverProperties.class.getClassLoader()
                    .getResource("flume-conf-with-envvars.properties").getFile());

    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
    private PropertiesFileConfigurationProvider provider;

    @BeforeMethod
    public void setUp() {
        provider = new PropertiesFileConfigurationProvider("a1", TESTFILE);
    }

    @Test
    public void resolveEnvVar() {
        environmentVariables.set("VARNAME", "varvalue");
        String resolved = EnvVarResolverProperties.resolveEnvVars("padding ${VARNAME} padding");
        Assert.assertEquals(resolved, "padding varvalue padding");
    }

    @Test
    public void resolveEnvVars() {
        environmentVariables.set("VARNAME1", "varvalue1");
        environmentVariables.set("VARNAME2", "varvalue2");
        String resolved = EnvVarResolverProperties
                .resolveEnvVars("padding ${VARNAME1} ${VARNAME2} padding");
        Assert.assertEquals(resolved, "padding varvalue1 varvalue2 padding");
    }

    @Test
    public void getProperty() {
        String NC_PORT = "6667";
        environmentVariables.set("NC_PORT", NC_PORT);
        System.setProperty("propertiesImplementation",
                "org.apache.pulsar.io.flume.node.EnvVarResolverProperties");

        Assert.assertEquals(provider.getFlumeConfiguration()
                .getConfigurationFor("a1")
                .getSourceContext().get("r1").getParameters().get("port"), NC_PORT);
    }
}
