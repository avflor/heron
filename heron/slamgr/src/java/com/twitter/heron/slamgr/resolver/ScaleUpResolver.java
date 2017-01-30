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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.IResolver;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.Runtime;


public class ScaleUpResolver implements IResolver<ComponentBottleneck> {

  private static final Logger LOG = Logger.getLogger(ScaleUpResolver.class.getName());

  private Config config;
  private Config runtime;
  private ISchedulerClient schedulerClient;

  @Override
  public void initialize(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;
    schedulerClient = getSchedulerClient(runtime);
  }

  @Override
  public Boolean resolve(Diagnosis<ComponentBottleneck> diagnosis, TopologyAPI.Topology topology) {

    if (diagnosis.getSummary() == null) {
      throw new RuntimeException("Not valid diagnosis object");
    }

    ComponentBottleneck bottleneck = diagnosis.getSummary().iterator().next();
    String componentName = bottleneck.getComponentName();

    String topologyName = topology.getName();
    LOG.fine(String.format("updateTopologyHandler called for %s with %s",
        topologyName, 3));

    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    Map<String, Integer> changeRequests = new HashMap<>();
    changeRequests.put(componentName, 1);
    PackingPlans.PackingPlan currentPlan = manager.getPackingPlan(topologyName);


    PackingPlans.PackingPlan proposedPlan = buildNewPackingPlan(currentPlan, changeRequests,
        topology);

    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .setCurrentPackingPlan(currentPlan)
            .setProposedPackingPlan(proposedPlan)
            .build();

    LOG.info("Sending Updating topology request: " + updateTopologyRequest);
    if (!schedulerClient.updateTopology(updateTopologyRequest)) {
      LOG.log(Level.SEVERE, "Failed to update topology with Scheduler, updateTopologyRequest="
          + updateTopologyRequest);
      return false;
    }

    // Clean the connection when we are done.
    LOG.fine("Scheduler updated topology successfully.");
    return true;

  }


  @Override
  public void close() {

  }

  protected ISchedulerClient getSchedulerClient(Config runtime) {
    return new SchedulerClientFactory(config, runtime).getSchedulerClient();
  }

  PackingPlans.PackingPlan buildNewPackingPlan(PackingPlans.PackingPlan currentProtoPlan,
                                               Map<String, Integer> changeRequests,
                                               TopologyAPI.Topology topology) {
    PackingPlanProtoDeserializer deserializer = new PackingPlanProtoDeserializer();
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    PackingPlan currentPackingPlan = deserializer.fromProto(currentProtoPlan);

    Map<String, Integer> componentCounts = currentPackingPlan.getComponentCounts();
    Map<String, Integer> componentChanges = parallelismDelta(componentCounts, changeRequests);

    // Create an instance of the packing class
    String repackingClass = Context.repackingClass(config);
    IRepacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(repackingClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate packing instance: " + repackingClass, e);
    }
    try {
      packing.initialize(config, topology);
      PackingPlan packedPlan = packing.repack(currentPackingPlan, componentChanges);
      return serializer.toProto(packedPlan);
    } finally {
      SysUtils.closeIgnoringExceptions(packing);
    }
  }

  Map<String, Integer> parallelismDelta(Map<String, Integer> componentCounts,
                                        Map<String, Integer> changeRequests) {
    Map<String, Integer> componentDeltas = new HashMap<>();
    for (String component : changeRequests.keySet()) {
      if (!componentCounts.containsKey(component)) {
        throw new IllegalArgumentException(
            "Invalid component name in update request: " + component);
      }
      Integer newValue = changeRequests.get(component);
      Integer delta = newValue - componentCounts.get(component);
      if (delta != 0) {
        componentDeltas.put(component, delta);
      }
    }
    return componentDeltas;
  }
}
