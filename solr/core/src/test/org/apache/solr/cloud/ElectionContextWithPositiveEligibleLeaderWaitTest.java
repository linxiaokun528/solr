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

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.solr.common.cloud.Replica;
import org.junit.BeforeClass;
import org.junit.Test;

/** A stale replica may become leader only after its configured eligibility wait. */
public class ElectionContextWithPositiveEligibleLeaderWaitTest extends ElectionContextTestBase {
  private static final int ELIGIBLE_LEADER_WAIT_MS = 10 * LEADER_VOTE_WAIT_MS;

  public ElectionContextWithPositiveEligibleLeaderWaitTest(
      @Name("replicaType") Replica.Type replicaType) {
    super(replicaType);
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return replicaTypes();
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster(ELIGIBLE_LEADER_WAIT_MS);
  }

  @Test
  public void testStaleReplicaBecomesLeaderAfterTimeout() throws Exception {
    stopNodesWithUnequalTerms();
    startStaleReplica();
    waitForLeader(follower);
    try (ZkShardTerms terms = new ZkShardTerms(collectionName, SHARD, cluster.getZkClient())) {
      assertEquals(highestTermBeforeElection, terms.getTerm(follower.getName()));
      assertEquals(highestTermBeforeElection, terms.getHighestTerm());
    }
    assertReplicaDocuments(follower, "baseline");
  }
}
