//  Copyright 2017 Twitter. All rights reserved.
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
package com.twitter.heron.healthmgr.resolver;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IResolver;
import com.twitter.heron.spi.healthmgr.InstanceSymptom;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;

public class SpoutScaleUpResolver implements IResolver<ComponentSymptom> {

  private static final String EMIT_COUNT_METRIC = "__emit-count/default";
  private static final Logger LOG = Logger.getLogger(SpoutScaleUpResolver.class.getName());

  private Config config;
  private Config runtime;
  private ISchedulerClient schedulerClient;
  private int newParallelism;
  private double maxEmitCount = 0;

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.config = inputConfig;
    this.runtime = inputRuntime;
    schedulerClient = (ISchedulerClient) Runtime.schedulerClientInstance(runtime);
  }

  public void setMaxEMitCount(double value) {
    this.maxEmitCount = value;
  }

  @Override
  public Boolean resolve(Diagnosis<ComponentSymptom> diagnosis, TopologyAPI.Topology topology) {

    ComponentSymptom symptom = diagnosis.getSummary().iterator().next();
    String componentName = symptom.getComponentName();

    String topologyName = topology.getName();
    LOG.fine(String.format("updateTopologyHandler called for %s with %s",
        topologyName, newParallelism));

    SchedulerStateManagerAdaptor manager = Runtime.schedulerStateManagerAdaptor(runtime);
    Map<String, Integer> changeRequests = new HashMap<>();
    changeRequests.put(componentName, this.newParallelism);
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
  public double estimateOutcome(Diagnosis<ComponentSymptom> diagnosis,
                                TopologyAPI.Topology topology) {
    if (diagnosis.getSummary() == null) {
      throw new RuntimeException("Not valid diagnosis object");
    }

    ComponentSymptom current = diagnosis.getSummary().iterator().next();
    double scaleFactor = computeScaleUpFactor(current);
    System.out.println(current.getComponentName() + " " + current.getInstances().size());
    this.newParallelism = (int) Math.ceil(current.getInstances().size() * scaleFactor);

    if (this.newParallelism == 0) {
      throw new RuntimeException("New parallelism after scale up is 0."
          + " Please set the parallelism value");
    }
    return scaleFactor;
  }


  private double computeScaleUpFactor(ComponentSymptom current) {
    double totalEmitCount = 0;
    for (InstanceSymptom instanceData : current.getInstances()) {
      double emitCount =
          Double.valueOf(instanceData.getDataPoint(EMIT_COUNT_METRIC));
      LOG.log(Level.INFO, "Instance: {0}, emit-count: {1}",
          new Object[]{instanceData.getInstanceData().getInstanceId(), emitCount});
      totalEmitCount += emitCount;
    }

    LOG.info("Total emit count: " + totalEmitCount);

    double scaleFactor = this.maxEmitCount / totalEmitCount;
    // scale up fencing: do not scale more than 4 times the current size
    scaleFactor = scaleFactor > 4.0 ? 4.0 : scaleFactor;
    LOG.info("Spout scale up factor: " + scaleFactor);

    return scaleFactor;
  }

  @Override
  public boolean successfulAction(Diagnosis<ComponentSymptom> oldDiagnosis,
                                  Diagnosis<ComponentSymptom> newDiagnosis, double improvement) {

    /*Set<ComponentSymptom> oldSummary = oldDiagnosis.getSummary();
    Set<ComponentSymptom> newSummary = newDiagnosis.getSummary();
    ComponentSymptom oldComponent = oldSummary.iterator().next();
    ComponentSymptom newComponent = newSummary.iterator().next();
    System.out.println("old " + oldComponent.toString());
    System.out.println("new " + newComponent.toString());

    if (SLAManagerUtils.improvedMetricSum(oldComponent, newComponent,
        EMIT_COUNT_METRIC, improvement)) {
      System.out.println("first");
      return true;
    }

    return false;*/
    return true;
  }

  @Override
  public void close() {
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
