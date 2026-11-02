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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.update.UpdateShardHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Checks eligibility deadlines and wait cancellation without wall-clock delays. */
public class ShardLeaderElectionWaitTest extends SolrTestCase {
  private static final String COLLECTION = "collection";
  private static final String SHARD = "shard1";
  private static final String CANDIDATE = "candidate";
  private static final String OTHER = "other";
  private static final String CORE = "candidate_core";
  private static final int WAIT_MS = 1_000;

  private CoreContainer coreContainer;
  private ZkController zkController;
  private ZkShardTerms terms;
  private ShardLeaderElectionContext context;

  @BeforeClass
  public static void checkMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void createContext() {
    coreContainer = mock(CoreContainer.class);
    zkController = mock(ZkController.class);
    terms = mock(ZkShardTerms.class);
    SolrZkClient zkClient = mock(SolrZkClient.class);
    ZkStateReader stateReader = mock(ZkStateReader.class);
    when(zkController.getZkClient()).thenReturn(zkClient);
    when(zkController.getZkStateReader()).thenReturn(stateReader);
    when(stateReader.getZkClient()).thenReturn(zkClient);
    when(coreContainer.isLoaded(CORE)).thenReturn(true);
    when(coreContainer.getUpdateShardHandler()).thenReturn(mock(UpdateShardHandler.class));
    when(coreContainer.getShardHandlerFactory()).thenReturn(mock(ShardHandlerFactory.class));
    when(terms.getTerm(CANDIDATE)).thenReturn(1L);
    when(terms.getTerm(OTHER)).thenReturn(2L);
    when(terms.getHighestTerm()).thenReturn(2L);
    setOtherNodeLive(false);
    context =
        new ShardLeaderElectionContext(
            mock(LeaderElector.class),
            SHARD,
            COLLECTION,
            CANDIDATE,
            new ZkNodeProps(ZkStateReader.CORE_NAME_PROP, CORE),
            zkController,
            coreContainer);
  }

  @After
  public void closeContext() {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void testZeroTimeoutDoesNotWait() throws Exception {
    TestTimeSource time = new TestTimeSource();
    assertTrue(waitForEligibleLeader(0, time));
    assertEquals(0, time.sleeps);
  }

  @Test
  public void testHigherTermOnDeadNodeDoesNotPreventTimeout() throws Exception {
    TestTimeSource time = new TestTimeSource();
    assertTrue(waitForEligibleLeader(WAIT_MS, time));
    assertTrue("Promoted before the configured deadline", time.elapsedMillis() >= WAIT_MS);
    assertTrue("Ignored the configured deadline", time.elapsedMillis() <= 2 * WAIT_MS);
  }

  @Test
  public void testMaximumTimeoutDoesNotOverflow() throws Exception {
    TestTimeSource time = new TestTimeSource();
    time.afterSleep = () -> when(terms.canBecomeLeader(CANDIDATE)).thenReturn(true);
    assertFalse(waitForEligibleLeader(Integer.MAX_VALUE, time));
    assertTrue("Must wait until eligibility changes", time.elapsedMillis() > 0);
    assertTrue(time.elapsedMillis() < Integer.MAX_VALUE);
  }

  @Test
  public void testNegativeTimeoutsWaitUntilEligibilityChanges() throws Exception {
    for (int timeout : new int[] {-1, -10, Integer.MIN_VALUE}) {
      when(terms.canBecomeLeader(CANDIDATE)).thenReturn(false);
      TestTimeSource time = new TestTimeSource();
      time.afterSleep =
          () -> {
            time.advance(TimeUnit.DAYS.toNanos(30));
            when(terms.canBecomeLeader(CANDIDATE)).thenReturn(true);
          };
      assertFalse(
          "Negative timeout must never permit stale promotion: " + timeout,
          waitForEligibleLeader(timeout, time));
      assertTrue(time.elapsedMillis() >= TimeUnit.DAYS.toMillis(30));
    }
  }

  @Test
  public void testLiveHigherTermReplicaTakesPrecedence() throws Exception {
    setOtherNodeLive(true);
    TestTimeSource time = new TestTimeSource();
    assertFalse(waitForEligibleLeader(WAIT_MS, time));
    assertEquals(0, time.sleeps);
  }

  @Test
  public void testHigherTermReplicaReturnsDuringWait() throws Exception {
    TestTimeSource time = new TestTimeSource();
    time.afterSleep = () -> setOtherNodeLive(true);
    assertFalse(waitForEligibleLeader(WAIT_MS, time));
    assertTrue(time.elapsedMillis() > 0);
    assertTrue(
        "The returning replica must be noticed before the deadline",
        time.elapsedMillis() < WAIT_MS);
  }

  @Test
  public void testRecoveringReplicaYieldsToRecoveredReplicaAtSameTerm() throws Exception {
    setOtherNodeLive(true);
    when(terms.getTerm(OTHER)).thenReturn(1L);
    when(terms.isRecovering(CANDIDATE)).thenReturn(true);
    TestTimeSource time = new TestTimeSource();
    assertFalse(waitForEligibleLeader(-1, time));
    assertEquals(0, time.sleeps);
  }

  @Test
  public void testTwoRecoveringReplicasAtSameTermWaitForTimeout() throws Exception {
    setOtherNodeLive(true);
    when(terms.getTerm(OTHER)).thenReturn(1L);
    when(terms.isRecovering(CANDIDATE)).thenReturn(true);
    when(terms.isRecovering(OTHER)).thenReturn(true);
    TestTimeSource time = new TestTimeSource();
    assertTrue(waitForEligibleLeader(WAIT_MS, time));
    assertTrue(time.elapsedMillis() >= WAIT_MS);
  }

  @Test
  public void testWaitAbortsWhenContextCloses() throws Exception {
    TestTimeSource time = new TestTimeSource();
    time.afterSleep = context::close;
    assertFalse(waitForEligibleLeader(-1, time));
    assertTrue(time.elapsedMillis() > 0);
  }

  @Test
  public void testWaitAbortsWhenNodeShutsDown() throws Exception {
    TestTimeSource time = new TestTimeSource();
    time.afterSleep = () -> when(coreContainer.isShutDown()).thenReturn(true);
    assertFalse(waitForEligibleLeader(-1, time));
    assertTrue(time.elapsedMillis() > 0);
  }

  @Test
  public void testWaitAbortsWhenCoreUnloads() throws Exception {
    TestTimeSource time = new TestTimeSource();
    time.afterSleep = () -> when(coreContainer.isLoaded(CORE)).thenReturn(false);
    assertFalse(waitForEligibleLeader(-1, time));
    assertTrue(time.elapsedMillis() > 0);
  }

  @Test
  public void testInterruptedWaitPropagatesInterruption() {
    TestTimeSource time = new TestTimeSource();
    time.interruption = new InterruptedException("Interrupted during leader eligibility wait");
    InterruptedException thrown =
        assertThrows(InterruptedException.class, () -> waitForEligibleLeader(-1, time));
    assertSame(time.interruption, thrown);
  }

  private boolean waitForEligibleLeader(int timeout, TestTimeSource time)
      throws InterruptedException {
    return context.waitForEligibleBecomeLeaderAfterTimeout(terms, CANDIDATE, timeout, time);
  }

  private void setOtherNodeLive(boolean live) {
    Replica candidate = replica(CANDIDATE, "candidate-node");
    Replica other = replica(OTHER, "other-node");
    Slice slice =
        new Slice(SHARD, Map.of(CANDIDATE, candidate, OTHER, other), Map.of(), COLLECTION);
    DocCollection collection =
        DocCollection.create(
            COLLECTION, Map.of(SHARD, slice), Map.of(), DocRouter.DEFAULT, 0, Instant.EPOCH, null);
    when(zkController.getClusterState())
        .thenReturn(
            new ClusterState(
                live ? Set.of("candidate-node", "other-node") : Set.of("candidate-node"),
                Map.of(COLLECTION, collection)));
  }

  private static Replica replica(String name, String node) {
    return new Replica(
        name,
        node,
        COLLECTION,
        SHARD,
        name + "_core",
        Replica.State.ACTIVE,
        Replica.Type.NRT,
        Map.of(ZkStateReader.BASE_URL_PROP, "http://localhost:8983/solr"));
  }

  /** Advances monotonic time and applies a cluster transition whenever the waiter pauses. */
  private static class TestTimeSource extends TimeSource {
    private static final long START_NS = TimeUnit.HOURS.toNanos(1);
    private long nowNs = START_NS;
    private int sleeps;
    private Runnable afterSleep = () -> {};
    private InterruptedException interruption;

    @Override
    public long getTimeNs() {
      return nowNs++;
    }

    @Override
    public long getEpochTimeNs() {
      return getTimeNs();
    }

    @Override
    public long[] getTimeAndEpochNs() {
      long now = getTimeNs();
      return new long[] {now, now};
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      if (interruption != null) {
        throw interruption;
      }
      assertTrue("Wait did not terminate after the deadline or cluster transition", ++sleeps < 100);
      advance(TimeUnit.MILLISECONDS.toNanos(ms));
      afterSleep.run();
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      return toUnit.convert(value, fromUnit);
    }

    private void advance(long nanos) {
      nowNs += nanos;
    }

    private long elapsedMillis() {
      return TimeUnit.NANOSECONDS.toMillis(nowNs - START_NS);
    }
  }
}
