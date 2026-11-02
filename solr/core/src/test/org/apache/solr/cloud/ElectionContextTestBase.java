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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Shared election checks for finite and indefinite waits for an eligible leader. */
public abstract class ElectionContextTestBase extends SolrCloudTestCase {
  private static final AtomicInteger collectionSequence = new AtomicInteger();
  protected static final String SHARD = "shard1";
  protected static final int LEADER_VOTE_WAIT_MS = 1_000;
  protected static final int NO_LEADER_WAIT_MS = 10 * LEADER_VOTE_WAIT_MS;

  protected final Replica.Type replicaType;
  protected String collectionName;
  protected Replica leader;
  protected Replica follower;
  protected JettySolrRunner leaderRunner;
  protected JettySolrRunner followerRunner;
  protected long highestTermBeforeElection;

  protected ElectionContextTestBase(Replica.Type replicaType) {
    this.replicaType = replicaType;
  }

  protected static Iterable<Object[]> replicaTypes() {
    return List.of(new Object[] {Replica.Type.NRT}, new Object[] {Replica.Type.TLOG});
  }

  protected static void setupCluster(int eligibleLeaderWait) throws Exception {
    String solrXml =
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML
            .replace("${leaderVoteWait:10000}", Integer.toString(LEADER_VOTE_WAIT_MS))
            .replace(
                "</solrcloud>",
                "<int name=\"eligibleLeaderWait\">" + eligibleLeaderWait + "</int></solrcloud>");
    configureCluster(2)
        .withSolrXml(solrXml)
        .addConfig("conf", configset("election-context"))
        .configure();
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      assertEquals(
          eligibleLeaderWait, runner.getCoreContainer().getZkController().getEligibleLeaderWait());
      assertEquals(
          LEADER_VOTE_WAIT_MS, runner.getCoreContainer().getZkController().getLeaderVoteWait());
    }
  }

  @Before
  public void createCollection() throws Exception {
    collectionName = "election_context_" + collectionSequence.incrementAndGet();
    CollectionAdminRequest.createCollection(
            collectionName,
            "conf",
            1,
            replicaType == Replica.Type.NRT ? 2 : 0,
            replicaType == Replica.Type.TLOG ? 2 : 0,
            0)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 2);
    leader = getCollectionState(collectionName).getSlice(SHARD).getLeader();
    follower =
        getCollectionState(collectionName)
            .replicaStream()
            .filter(replica -> !replica.isLeader())
            .findFirst()
            .orElseThrow();
    leaderRunner = cluster.getReplicaJetty(leader);
    followerRunner = cluster.getReplicaJetty(follower);
    cluster.getSolrClient().add(collectionName, sdoc("id", "baseline"));
    cluster.getSolrClient().commit(collectionName);
    assertReplicaDocuments(leader, "baseline");
    assertReplicaDocuments(follower, "baseline");
    try (ZkShardTerms terms = new ZkShardTerms(collectionName, SHARD, cluster.getZkClient())) {
      assertTrue(terms.getHighestTerm() > 0);
      assertEquals(terms.getTerm(leader.getName()), terms.getTerm(follower.getName()));
    }
  }

  @After
  public void cleanCollection() throws Exception {
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      if (runner.isStopped()) {
        runner.start();
      }
    }
    cluster.waitForActiveCollection(collectionName, DEFAULT_TIMEOUT, TimeUnit.SECONDS);
    cluster.deleteAllCollections();
  }

  protected void stopNode(JettySolrRunner runner) throws Exception {
    String nodeName = runner.getNodeName();
    runner.stop();
    cluster
        .getZkStateReader()
        .waitForLiveNodes(
            DEFAULT_TIMEOUT, TimeUnit.SECONDS, (oldNodes, nodes) -> !nodes.contains(nodeName));
  }

  /** Leave both nodes stopped, with one additional update only on the former leader. */
  protected void stopNodesWithUnequalTerms() throws Exception {
    stopNode(followerRunner);
    cluster.getSolrClient().add(collectionName, sdoc("id", "leader_only"));
    cluster.getSolrClient().commit(collectionName);
    try (ZkShardTerms terms = new ZkShardTerms(collectionName, SHARD, cluster.getZkClient())) {
      assertTrue(terms.getTerm(leader.getName()) > terms.getTerm(follower.getName()));
      highestTermBeforeElection = terms.getHighestTerm();
    }
    stopNode(leaderRunner);
  }

  protected void startStaleReplica() throws Exception {
    followerRunner.start();
    waitForState(
        "The stale replica must enter the election without a leader",
        collectionName,
        (liveNodes, collection) ->
            collection != null
                && liveNodes.contains(follower.getNodeName())
                && collection.getSlice(SHARD).getLeader() == null);
  }

  protected void assertNoLeaderFor(int timeoutMs) {
    expectThrows(
        TimeoutException.class,
        () ->
            cluster
                .getZkStateReader()
                .waitForState(
                    collectionName,
                    timeoutMs,
                    TimeUnit.MILLISECONDS,
                    (liveNodes, collection) -> {
                      Replica currentLeader =
                          collection == null ? null : collection.getSlice(SHARD).getLeader();
                      return currentLeader != null
                          && liveNodes.contains(currentLeader.getNodeName());
                    }));
  }

  protected void waitForLeader(Replica expectedLeader) {
    waitForState(
        "Expected replica " + expectedLeader.getName() + " to become leader",
        collectionName,
        (liveNodes, collection) -> {
          Replica currentLeader =
              collection == null ? null : collection.getSlice(SHARD).getLeader();
          return currentLeader != null
              && currentLeader.getName().equals(expectedLeader.getName())
              && currentLeader.isActive(liveNodes);
        });
  }

  protected void assertReplicaDocuments(Replica replica, String... ids) throws Exception {
    Set<String> expected = Set.of(ids);
    new TimeOut(DEFAULT_TIMEOUT, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "Expected documents " + expected + " on replica " + replica.getName(),
            () -> {
              try {
                var documents =
                    cluster
                        .getReplicaJetty(replica)
                        .getSolrClient()
                        .query(
                            replica.getCoreName(),
                            params("q", "*:*", "distrib", "false", "fl", "id"))
                        .getResults();
                return documents.getNumFound() == expected.size()
                    && documents.stream()
                        .map(doc -> doc.getFieldValue("id"))
                        .collect(Collectors.toSet())
                        .equals(expected);
              } catch (SolrServerException | IOException e) {
                throw new AssertionError("Cannot query replica " + replica.getName(), e);
              }
            });
  }

  @Test
  public void testEqualTermReplicaBecomesLeaderAfterLeaderStops() throws Exception {
    stopNode(leaderRunner);
    waitForLeader(follower);
    assertReplicaDocuments(follower, "baseline");
  }

  @Test
  public void testEqualTermReplicaBecomesLeaderAfterClusterRestart() throws Exception {
    stopNode(followerRunner);
    stopNode(leaderRunner);
    followerRunner.start();
    waitForLeader(follower);
    assertReplicaDocuments(follower, "baseline");
  }

  @Test
  public void testHigherTermReplicaBecomesLeaderAfterClusterRestart() throws Exception {
    stopNodesWithUnequalTerms();
    leaderRunner.start();
    waitForLeader(leader);
    assertReplicaDocuments(leader, "baseline", "leader_only");
  }

  @Test
  public void testHigherTermReplicaReturnsWhileStaleReplicaWaits() throws Exception {
    stopNodesWithUnequalTerms();
    startStaleReplica();
    leaderRunner.start();
    waitForLeader(leader);
    cluster.waitForActiveCollection(collectionName, 1, 2);
    assertReplicaDocuments(leader, "baseline", "leader_only");
    assertReplicaDocuments(follower, "baseline", "leader_only");
  }

  @Test
  public void testForceLeaderReleasesWaitingReplica() throws Exception {
    stopNodesWithUnequalTerms();
    startStaleReplica();
    CollectionAdminRequest.forceLeaderElection(collectionName, SHARD)
        .process(cluster.getSolrClient());
    waitForLeader(follower);
    try (ZkShardTerms terms = new ZkShardTerms(collectionName, SHARD, cluster.getZkClient())) {
      assertTrue(terms.canBecomeLeader(follower.getName()));
      assertTrue(terms.getTerm(follower.getName()) >= highestTermBeforeElection);
    }
    assertReplicaDocuments(follower, "baseline");
  }
}
