/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/** An indefinite eligibility wait keeps a stale replica from promoting itself. */
public class ElectionContextWithNegativeEligibleLeaderWaitTest extends ElectionContextTestBase {
  private static final int UNLOAD_TIMEOUT_MS = NO_LEADER_WAIT_MS;

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster(-1);
  }

  @Test
  public void testStaleReplicaWaitsForEligibleLeader() throws Exception {
    stopNodesWithUnequalTerms();
    startStaleReplica();
    assertNoLeaderFor(NO_LEADER_WAIT_MS);
    try (ZkShardTerms terms = new ZkShardTerms(collectionName, SHARD, cluster.getZkClient())) {
      assertFalse(terms.canBecomeLeader(follower.getName()));
    }
    leaderRunner.start();
    waitForLeader(leader);
    cluster.waitForActiveCollection(collectionName, 1, 2);
  }

  @Test
  public void testUnloadWhileWaitingForEligibleLeader() throws Exception {
    stopNodesWithUnequalTerms();
    startStaleReplica();
    assertNoLeaderFor(NO_LEADER_WAIT_MS);
    assertTrue(followerRunner.getCoreContainer().isLoaded(follower.getCoreName()));

    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("unload-waiting-replica"));
    try {
      Future<?> unload =
          executor.submit(() -> followerRunner.getCoreContainer().unload(follower.getCoreName()));
      unload.get(UNLOAD_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertFalse(followerRunner.getCoreContainer().isLoaded(follower.getCoreName()));
      waitForState(
          "Unloaded replica must leave the collection",
          collectionName,
          collection -> collection != null && collection.getReplica(follower.getName()) == null);
    } finally {
      try {
        leaderRunner.start();
      } finally {
        ExecutorUtil.shutdownNowAndAwaitTermination(executor);
      }
    }
  }
}
