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

package com.twitter.heron.packing.graphpartitioning;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.grouping.DirectMappingGrouping;
import com.twitter.heron.packing.TestBolt;
import com.twitter.heron.packing.TestSpout;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.PackingPlan;


public class GraphPartitioningPackingTest {

  private long instanceRamDefault;
  private double instanceCpuDefault;
  private long instanceDiskDefault;

  protected PackingPlan getGraphPartitioningPackingPlan(TopologyAPI.Topology topology) {
    com.twitter.heron.spi.common.Config config = com.twitter.heron.spi.common.Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        //   .put("heron.scheduler.local.working.directory", "/home/avrilia/")
        .putAll(ClusterDefaults.getDefaults())
        .build();

    com.twitter.heron.spi.common.Config runtime = com.twitter.heron.spi.common.Config.newBuilder()
        .put(Keys.topologyDefinition(), topology)
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    GraphPartitioningPacking packing = new GraphPartitioningPacking();
    packing.initialize(config, topology);
    PackingPlan output = packing.pack();

    return output;
  }


  protected TopologyAPI.Topology getTopology3(Config conf) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 6);

    topologyBuilder.setBolt("test-bolt", new TestBolt(), 3).
        customGrouping("test-spout", new DirectMappingGrouping());

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    return fTopology;
  }


  protected TopologyAPI.Topology getTopology2(Config conf) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 3);

    topologyBuilder.setBolt("test-bolt", new TestBolt(), 6).
        customGrouping("test-spout", new DirectMappingGrouping());

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    return fTopology;
  }

  protected TopologyAPI.Topology getTopology(Config conf) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 3);

    topologyBuilder.setBolt("test-bolt", new TestBolt(), 3).
        shuffleGrouping("test-spout");

    topologyBuilder.setBolt("test-bolt2", new TestBolt(), 2).
        shuffleGrouping("test-bolt");

    topologyBuilder.setBolt("test-bolt3", new TestBolt(), 2).
        shuffleGrouping("test-spout");

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    return fTopology;
  }

  @Test(expected = RuntimeException.class)
  public void testCheckFailure() throws Exception {
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    // Explicit set insufficient ram for container
    long containerRam = -1L * Constants.GB;

    topologyConfig.setContainerMaxRamHint(containerRam);

    TopologyAPI.Topology topology =
        getTopology(topologyConfig);
    PackingPlan packingPlan =
        getGraphPartitioningPackingPlan(topology);
  }

  /**
   * Test the scenario where the max container size is the default
   */
  @Test
  public void testDefaultResources() throws Exception {
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();

    // No explicit resources required
    TopologyAPI.Topology topologyNoExplicitResourcesConfig =
        getTopology(topologyConfig);

    PackingPlan packingPlan =
        getGraphPartitioningPackingPlan(topologyNoExplicitResourcesConfig);

    Assert.assertEquals(packingPlan.getContainers().size(), 3);
  }

  /**
   * Test the scenario where the max container size is the default and padding is configured
   */
  @Test
  public void testDefaultContainerSizeWithPadding() throws Exception {

    int padding = 50;
    // Set up the topology and its config
    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    topologyConfig.setContainerPaddingPercentage(padding);
    TopologyAPI.Topology topology =
        getTopology(topologyConfig);

    PackingPlan packingPlan =
        getGraphPartitioningPackingPlan(topology);

    Assert.assertEquals(packingPlan.getContainers().size(), 3);
  }

  /**
   * Test the scenario container level resource config are set
   */
  @Test
  public void test() {


    com.twitter.heron.api.Config topologyConfig = new com.twitter.heron.api.Config();
    TopologyAPI.Topology fTopology = getTopology3(topologyConfig);

    PackingPlan packingPlan =
        getGraphPartitioningPackingPlan(fTopology);

    // Set instances
    // Construct the spoutInstance
    /*PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo =
    PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo.setComponentName("test-spout");
    spoutInstanceInfo.setTaskId(0);
    spoutInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance = PhysicalPlans.Instance.newBuilder();
    spoutInstance.setInstanceId("spout-id");
    spoutInstance.setStmgrId("stream-manager-id");
    spoutInstance.setInfo(spoutInstanceInfo);

    // Construct the boltInstanceInfo
    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo.setComponentName("test-bolt");
    boltInstanceInfo.setTaskId(1);
    boltInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance = PhysicalPlans.Instance.newBuilder();
    boltInstance.setInstanceId("bolt-id");
    boltInstance.setStmgrId("stream-manager-id");
    boltInstance.setInfo(boltInstanceInfo);*/
  }
}
