/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.insight.datanode;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Insight point for debugging Datanode container protocol.
 */
public class DatanodeContainerInsight extends BaseInsightPoint {

  private final OzoneConfiguration conf;

  public DatanodeContainerInsight(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {
    try (ScmClient scmClient = createScmClient(conf)) {
      DatanodeDetails datanode = scmClient.listPipelines().stream()
          .flatMap(p -> p.getNodes().stream())
          // TODO: add filter
          .findAny()
          .orElseThrow(() -> new IllegalStateException("No datanode found"));
      Component component = new Component(
          Type.DATANODE, datanode.getUuid().toString(),
          datanode.getHostName(), 9882);
      LoggerSource.Level logLevel = defaultLevel(verbose);
      return ImmutableList.of(
          new LoggerSource(component, HddsDispatcher.class, logLevel)
      );
    } catch (IOException e) {
      throw new UncheckedIOException("Can't enumerate required logs", e);
    }
  }

  @Override
  public String getDescription() {
    return "Datanode Container protocol endpoint";
  }
}
