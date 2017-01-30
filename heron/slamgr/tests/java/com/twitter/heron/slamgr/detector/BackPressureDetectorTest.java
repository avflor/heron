//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License

package com.twitter.heron.slamgr.detector;

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.slamgr.utils.TestBolt;
import com.twitter.heron.slamgr.utils.TestSpout;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigKeys;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    TopologyUtils.class, ReflectionUtils.class, TopologyAPI.Topology.class})
public class BackPressureDetectorTest {


  private static final String STATE_MANAGER_CLASS = "STATE_MANAGER_CLASS";
  private IStateManager stateManager;
  private Config config;
  private TopologyAPI.Topology topology;


  public static TopologyAPI.Topology getTopology(String topologyName) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout("word", new TestSpout(), 2);

    topologyBuilder.setBolt("exclaim1", new TestBolt(), 2).
        shuffleGrouping("word");

    //topologyBuilder.setBolt("test-bolt2", new TestBolt(), 3).
        //shuffleGrouping("test-bolt");

    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, 2);

   /* Map<String, Integer> spouts = new HashMap<>();
    spouts.put("testSpout", 2);

    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("testBolt", 3);

    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, 1);

    TopologyAPI.Topology topology =
        TopologyTests.createTopology(topologyName, topologyConfig, spouts, bolts);*/

    TopologyAPI.Topology topology =
        topologyBuilder.createTopology().
            setName(topologyName).
            setConfig(topologyConfig).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();
    return topology;
  }

  public static PackingPlan getPackingPlan(TopologyAPI.Topology topology, IPacking packing) {

    Config config = Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .putAll(ClusterDefaults.getDefaults())
        .build();

    packing.initialize(config, topology);
    return packing.pack();
  }

  public static PackingPlans.PackingPlan testProtoPackingPlan(
      TopologyAPI.Topology topology, IPacking packing) {
    PackingPlan plan = getPackingPlan(topology, packing);
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(plan);
  }

  private SettableFuture<PackingPlans.PackingPlan> getTestPacking(TopologyAPI.Topology topology) {
    PackingPlans.PackingPlan packingPlan =
        testProtoPackingPlan(topology, new RoundRobinPacking());
    final SettableFuture<PackingPlans.PackingPlan> future = SettableFuture.create();
    future.set(packingPlan);
    return future;
  }

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = getTopology("DataSkewTopology");
    config = mock(Config.class);
    when(config.getStringValue(ConfigKeys.get("STATE_MANAGER_CLASS"))).
        thenReturn(STATE_MANAGER_CLASS);

    // Mock objects to be verified
    stateManager = mock(IStateManager.class);

    final SettableFuture<PackingPlans.PackingPlan> future = getTestPacking(this.topology);
    when(stateManager.getPackingPlan(null, "DataSkewTopology")).thenReturn(future);

    // Mock ReflectionUtils stuff
    PowerMockito.spy(ReflectionUtils.class);
    PowerMockito.doReturn(stateManager).
        when(ReflectionUtils.class, "newInstance", STATE_MANAGER_CLASS);
  }

  @Test
  public void testDetector() {

    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(config, topology);

    BackPressureDetector detector = new BackPressureDetector();
    detector.initialize(config, visitor);

    Diagnosis<ComponentBottleneck> result = detector.detect(topology);
    Assert.assertEquals(1, result.getSummary().size());
  }
}
