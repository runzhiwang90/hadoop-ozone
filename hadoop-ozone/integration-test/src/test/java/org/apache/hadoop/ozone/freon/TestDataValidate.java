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

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.Test;

import static org.apache.hadoop.ozone.freon.TestRandomKeyGenerator.runTest;

/**
 * Tests Freon, with MiniOzoneCluster and validate data.
 */
public abstract class TestDataValidate {

  private static MiniOzoneCluster cluster;

  static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5).setTotalPipelineNumLimit(8).build();
    cluster.waitForClusterToBeReady();
  }

  static void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void ratisTestLargeKey() throws Exception {
    runTest(1, 1, 1, 20971520, 10, true,
        cluster.getConf());
  }

  @Test
  public void validateWriteTest() throws Exception {
    runTest(2, 5, 10, 10240, 10, true,
        cluster.getConf());
  }

}
