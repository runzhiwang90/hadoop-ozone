/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.freon;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

/**
 * Tests Freon, with MiniOzoneCluster.
 */
public class TestRandomKeyGenerator {

  private static MiniOzoneCluster cluster;

  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
      .setNameFormat("RandomKeyGenerator-%d")
      .build();

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static void runInBackground(RandomKeyGenerator subject) {
    THREAD_FACTORY.newThread(subject::run).start();
  }

  static void runTest(int volumes, int buckets, int keys,
      long keySize, int threads, boolean validate, Configuration config)
      throws IOException, InterruptedException, TimeoutException {

    RandomKeyGenerator subject = new RandomKeyGenerator();
    subject.setNumOfVolumes(volumes);
    subject.setNumOfBuckets(buckets);
    subject.setNumOfKeys(keys);
    subject.setKeySize(keySize);
    subject.setFactor(ReplicationFactor.THREE);
    subject.setType(ReplicationType.RATIS);
    subject.setValidateWrites(validate);
    subject.setNumOfThreads(threads);
    subject.init(config);

    runInBackground(subject);

    int expectedBuckets = volumes * buckets;
    int expectedKeys = expectedBuckets * keys;

    Supplier<Boolean> check = subject.getValidateWrites()
        ? () -> expectedKeys == subject.getTotalKeysValidated()
        : () -> expectedKeys == subject.getNumberOfKeysAdded();
    int timeout = (int) (expectedKeys * Math.max(32, Math.sqrt(keySize)) * 13);
    if (subject.getValidateWrites()) {
      timeout *= 2;
    }
    GenericTestUtils.waitFor(check, 100, timeout);

    assertEquals(volumes, subject.getNumberOfVolumesCreated());
    assertEquals(expectedBuckets, subject.getNumberOfBucketsCreated());
    assertEquals(expectedKeys, subject.getNumberOfKeysAdded());
    assertEquals(Math.min(expectedKeys, threads), subject.getThreadPoolSize());

    if (subject.getValidateWrites()) {
      assertEquals(expectedKeys, subject.getTotalKeysValidated());
      assertEquals(expectedKeys, subject.getSuccessfulValidationCount());
      assertEquals(0, subject.getUnsuccessfulValidationCount());
    }
  }

  @Test
  public void fewRegularKeys() throws Exception {
    runTest(2, 4, 8, 10240, 4, false,
        cluster.getConf());
  }

  @Test
  public void manySmallKeys() throws Exception {
    runTest(8, 16, 128, 16, 8, false,
        cluster.getConf());
  }

  @Test
  public void oneHugeKey() throws Exception {
    runTest(1, 1, 1, 10L + Integer.MAX_VALUE, 10, true,
        cluster.getConf());
  }

  @Test
  public void emptyKey() throws Exception {
    runTest(1, 1, 1, 0, 10, true,
        cluster.getConf());
  }

}
