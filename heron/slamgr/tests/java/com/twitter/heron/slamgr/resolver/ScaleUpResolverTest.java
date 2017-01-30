
// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.slamgr.resolver;

import java.security.Key;

import com.google.common.util.concurrent.SettableFuture;

import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.slamgr.detector.BackPressureDetector;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.slamgr.utils.TestBolt;
import com.twitter.heron.slamgr.utils.TestSpout;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.utils.LauncherUtils;
import com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScaleUpResolverTest {
  private IStateManager stateManager;
  private TopologyAPI.Topology topology;


  public static TopologyAPI.Topology getTopology(String topologyName) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();

    topologyBuilder.setSpout("word", new TestSpout(), 2);

    topologyBuilder.setBolt("exclaim1", new TestBolt(), 2).
        shuffleGrouping("word");


    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.put(com.twitter.heron.api.Config.TOPOLOGY_STMGRS, 2);

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
    this.topology = getTopology("ds");
  }

  @Test
  public void testResolver() {
    Config config = Config.newBuilder()
        .put(Keys.repackingClass(), ResourceCompliantRRPacking.class.getName())
        .put(Keys.instanceCpu(), "1")
        .put(Keys.instanceRam(), 192L * Constants.MB)
        .put(Keys.instanceDisk(), 1024L * Constants.MB)
        .put(Keys.stateManagerRootPath(), "/Users/heron/.herondata/repository/state/local")
        .put(Keys.stateManagerClass(), LocalFileSystemStateManager.class.getName())
        .build();

    stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);
    SchedulerStateManagerAdaptor adaptor =
        new SchedulerStateManagerAdaptor(stateManager, 5000);

    Config runtime = Config.newBuilder()
        .put(Keys.schedulerStateManagerAdaptor(), adaptor)
        .put(Keys.topologyName(), "ds")
        .build();

    ISchedulerClient schedulerClient = new SchedulerClientFactory(config, runtime).getSchedulerClient();

    runtime = Config.newBuilder()
        .putAll(runtime)
        .put(Keys.schedulerClientInstance(), schedulerClient)
        .build();

    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(config, topology);

    BackPressureDetector detector = new BackPressureDetector();
    detector.initialize(config, runtime, visitor);

    Diagnosis<ComponentBottleneck> result = detector.detect(topology);
    Assert.assertEquals(1, result.getSummary().size());

    ScaleUpResolver resolver = new ScaleUpResolver();
    resolver.initialize(config, runtime);

    resolver.resolve(result, topology);
  }
}