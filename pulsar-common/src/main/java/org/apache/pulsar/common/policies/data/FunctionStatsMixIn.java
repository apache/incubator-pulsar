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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({"receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal",
        "avgProcessLatency", "1min", "lastInvocation", "instances"})
public abstract class FunctionStatsMixIn {
    @JsonProperty("1min")
    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({ "instanceId", "metrics" })
    public abstract static class FunctionInstanceStatsMixIn {

        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
                "userExceptionsTotal", "avgProcessLatency" })
        public abstract static class FunctionInstanceStatsDataBaseMixIn {

        }

        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
                "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "userMetrics" })
        public abstract static class FunctionInstanceStatsDataMixIn extends FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase {
            @JsonProperty("1min")
            public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin;
        }

    }
}
