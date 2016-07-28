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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.packing.TestBolt;
import com.twitter.heron.packing.TestSpout;
import com.twitter.heron.spi.common.ClusterDefaults;
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
        .putAll(ClusterDefaults.getDefaults())
        .build();

    com.twitter.heron.spi.common.Config runtime = com.twitter.heron.spi.common.Config.newBuilder()
        .put(Keys.topologyDefinition(), topology)
        .build();

    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);

    GraphPartitioningPacking packing = new GraphPartitioningPacking();
    packing.initialize(config, runtime);
    PackingPlan output = packing.pack();

    return output;
  }
  /**
   * Test the scenario container level resource config are set
   */
  @Test
  public void test() {
    // Set topology protobuf
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 2);

    topologyBuilder.setBolt("test-bolt", new TestBolt(), 2).
        shuffleGrouping("test-spout");

    topologyBuilder.setBolt("test-bolt2", new TestBolt(), 2).
        shuffleGrouping("test-bolt");

    topologyBuilder.setBolt("test-bolt3", new TestBolt(), 2).
        shuffleGrouping("test-spout");

    Config conf = new Config();
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setEnableAcking(false);

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();


    System.out.println("OS + " + fTopology.getSpouts(0).getOutputsList());

    System.out.println("IS + " + fTopology.getBolts(0).getOutputsList());
    System.out.println("IS 1+ " + fTopology.getBolts(1).getOutputsList());

    System.out.println("OS + " + fTopology.getBolts(0).getInputsList());
    System.out.println("OS 1+ " + fTopology.getBolts(1).getInputsList());

    Map<String, TopologyAPI.Bolt.Builder> bolts = new HashMap<>();
    Map<String, TopologyAPI.Spout.Builder> spouts = new HashMap<>();
    Map<String, HashSet<String>> prev = new HashMap<>();

    // We will build the structure of the topologyBlr - a graph directed from children to parents,
    // by looking only on bolts, since spout will not have parents
    for (TopologyAPI.Bolt.Builder bolt : fTopology.toBuilder().getBoltsBuilderList()) {
      String name = bolt.getComp().getName();

      bolts.put(name, bolt);
      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        System.out.println(name + " " + parent + " " + inputStream.getGtype());
        if (prev.containsKey(name)) {
          prev.get(name).add(parent);
        } else {
          HashSet<String> parents = new HashSet<String>();
          parents.add(parent);
          prev.put(name, parents);
        }
      }
    }
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
