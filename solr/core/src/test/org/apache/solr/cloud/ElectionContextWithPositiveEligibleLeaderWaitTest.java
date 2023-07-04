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

import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Test;

/** A stale replica may become leader only after its configured eligibility wait. */
public class ElectionContextWithPositiveEligibleLeaderWaitTest extends ElectionContextTestBase {
  private static final int ELIGIBLE_LEADER_WAIT_MS = 10 * LEADER_VOTE_WAIT_MS;

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster(ELIGIBLE_LEADER_WAIT_MS);
  }

  @Test
  public void testStaleReplicaBecomesLeaderAfterTimeout() throws Exception {
    stopNodesWithUnequalTerms();
    long start = System.nanoTime();
    startStaleReplica();
    waitForLeader(follower);
    assertTrue(
        "The stale replica became leader before eligibleLeaderWait elapsed",
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= ELIGIBLE_LEADER_WAIT_MS);
    try (ZkShardTerms terms = new ZkShardTerms(collectionName, SHARD, cluster.getZkClient())) {
      assertEquals(terms.getHighestTerm(), terms.getTerm(follower.getName()));
    }
  }
}
