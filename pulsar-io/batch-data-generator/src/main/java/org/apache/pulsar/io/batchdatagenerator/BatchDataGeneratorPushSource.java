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

package org.apache.pulsar.io.batchdatagenerator;

import io.codearte.jfairy.Fairy;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.BatchPushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class BatchDataGeneratorPushSource extends BatchPushSource<Person> implements Runnable {

  private Fairy fairy;
  private SourceContext sourceContext;
  private int maxRecordsPerCycle = 10;

  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @Override
  public void close() {
    executor.shutdownNow();
  }

  @Override
  public void open(Map config, SourceContext context) throws Exception {
    this.fairy = Fairy.create();
    this.sourceContext = context;
  }

  @Override
  public void discover(Consumer taskEater) throws Exception {
    log.info("Generating one task for each instance");
    for (int i = 0; i < sourceContext.getNumInstances(); ++i) {
      taskEater.accept(String.format("something-%d", System.currentTimeMillis()).getBytes());
    }
  }

  @Override
  public void prepare(byte[] instanceSplit) throws Exception {
    log.info("Instance " + sourceContext.getInstanceId() + " got a new discovered task {}", new String(instanceSplit));
    executor.submit(this);
  }

  @Override
  public void run() {
    try {
      ByteBuffer val = sourceContext.getState("count");
      String currCount;
      if (val == null) {
        currCount = "0";
      } else {
        currCount = new String(val.array());
      }

      for (int i = 0; i < maxRecordsPerCycle; i++) {
        sourceContext.putState("count", ByteBuffer.wrap(String.valueOf(Integer.parseInt(currCount) + 1).getBytes()));
        Thread.sleep(50);
        Record<Person> record = new Record<Person>() {
          @Override
          public Optional<String> getKey() {
            return Optional.of(currCount);
          }

          @Override
          public Person getValue() {
            return new Person(fairy.person());
          }
        };
        consume(record);
      }
      // this task is completed
      consume(null);
    } catch (Exception e) {
      notifyError(e);
    }
  }
}
