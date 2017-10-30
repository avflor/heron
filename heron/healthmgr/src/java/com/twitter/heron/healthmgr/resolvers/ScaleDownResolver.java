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
package com.twitter.heron.healthmgr.resolvers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.common.InstanceInfo;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.utils.ReflectionUtils;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_OVER_PROVISIONING_SMALLWAITQ;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_OVER_PROVISIONING_UNSATCOMP;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;


public class ScaleDownResolver implements IResolver {
  public static final String CONF_SCALE_DOWN = "ScaleDownResolver.scaleDownThreshold";
  private static final Logger LOG = Logger.getLogger(ScaleDownResolver.class.getName());
  private TopologyProvider topologyProvider;
  private PackingPlanProvider packingPlanProvider;
  private ISchedulerClient schedulerClient;
  private EventManager eventManager;
  private Config config;
  private int scaleDownConf;

  @Inject
  public ScaleDownResolver(TopologyProvider topologyProvider,
                           PackingPlanProvider packingPlanProvider,
                           ISchedulerClient schedulerClient,
                           EventManager eventManager,
                           Config config,
                           HealthPolicyConfig policyConfig) {
    this.topologyProvider = topologyProvider;
    this.packingPlanProvider = packingPlanProvider;
    this.schedulerClient = schedulerClient;
    this.eventManager = eventManager;
    this.config = config;
    this.scaleDownConf = (int) policyConfig.getConfig(CONF_SCALE_DOWN, 5);

  }

  @Override
  public List<Action> resolve(List<Diagnosis> diagnosis) {
    for (Diagnosis diagnoses : diagnosis) {
      Symptom ovUnsatCompSymptom = diagnoses.getSymptoms().get
          (SYMPTOM_OVER_PROVISIONING_UNSATCOMP.text());
      Symptom ovSmallWaitQSymptom = diagnoses.getSymptoms().get(
          SYMPTOM_OVER_PROVISIONING_SMALLWAITQ.text());
      Symptom overprovisioningSymptom
          = ovUnsatCompSymptom != null ? ovUnsatCompSymptom : ovSmallWaitQSymptom;

      if (overprovisioningSymptom == null) {
        continue;
      }

      ComponentMetrics componentMetrics = overprovisioningSymptom.getComponentMetrics();
      if (componentMetrics.getMetrics().isEmpty()) {
        continue;
      }

      if (componentMetrics.getComponentNames().size() > 1) {
        throw new UnsupportedOperationException("Multiple components are overprovisioned. This "
            + "resolver expects as input one component");
      }

      String ovComponent = componentMetrics.getComponentNames().iterator().next();
      int newParallelism = computeScaleDownFactor(componentMetrics, overprovisioningSymptom);
      Map<String, Integer> changeRequest = new HashMap<>();
      changeRequest.put(ovComponent, newParallelism);

      PackingPlan currentPackingPlan = packingPlanProvider.get();
      PackingPlan newPlan = buildNewPackingPlan(changeRequest, currentPackingPlan);
      if (newPlan == null) {
        return null;
      }

      Scheduler.UpdateTopologyRequest updateTopologyRequest =
          Scheduler.UpdateTopologyRequest.newBuilder()
              .setCurrentPackingPlan(getSerializedPlan(currentPackingPlan))
              .setProposedPackingPlan(getSerializedPlan(newPlan))
              .build();

      LOG.info("Sending Updating topology request: " + updateTopologyRequest);
      if (!schedulerClient.updateTopology(updateTopologyRequest)) {
        throw new RuntimeException(String.format("Failed to update topology with Scheduler, "
            + "updateTopologyRequest=%s", updateTopologyRequest));
      }

      TopologyUpdate action = new TopologyUpdate();
      LOG.info("Broadcasting topology update event");
      eventManager.onEvent(action);

      LOG.info("Scheduler updated topology successfully.");

      List<Action> actions = new ArrayList<>();
      actions.add(action);
      return actions;
    }

    return null;
  }

  @VisibleForTesting
  PackingPlan buildNewPackingPlan(Map<String, Integer> changeRequest, PackingPlan currentPackingPlan) {
    // Create an instance of the packing class
    IRepacking algo = getRepackingClass(Context.repackingClass(config));
    return ScaleUpResolver
        .buildNewPackingPlan(changeRequest, currentPackingPlan, topologyProvider, config, algo);
  }

  @VisibleForTesting
  IRepacking getRepackingClass(String repackingClass) {
    IRepacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(repackingClass);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Failed to instantiate packing instance: " + repackingClass, e);
    }
    return packing;
  }

  @VisibleForTesting
  int computeScaleDownFactor(ComponentMetrics componentMetrics, Symptom symptom) {
    String componentName = componentMetrics.getComponentNames().iterator().next();
    int parallelism = 0;
    int currentNoInstances = getInstanceCount(componentMetrics);
    if (symptom.getSymptomName().equals(SYMPTOM_OVER_PROVISIONING_SMALLWAITQ.text())) {
      parallelism = (int) Math.ceil(currentNoInstances * (100 -
          scaleDownConf) / 100.0);
    } else if (symptom.getSymptomName().equals(SYMPTOM_OVER_PROVISIONING_UNSATCOMP.text())) {
      int currentTotalProcessingRate = 0;
      double maxProcessingRateObserved = symptom.getStats().get(componentName).getMetricAvg();
      ComponentMetrics exeCountMetrics = componentMetrics.filterByMetric(METRIC_EXE_COUNT.text());
      for (InstanceMetrics instanceMetrics : exeCountMetrics.getMetrics()) {
        Double metricValue = instanceMetrics.getValueSum();
        currentTotalProcessingRate += metricValue;
      }
      System.out.println(maxProcessingRateObserved + " " + currentTotalProcessingRate);
      parallelism = (int) Math.ceil(currentTotalProcessingRate / maxProcessingRateObserved);
    }

    LOG.info(String.format("Component's, %s new parallelism is: %d",
        componentName, parallelism));
    return parallelism;
  }

  private int getInstanceCount(ComponentMetrics componentMetrics) {
    return (int) componentMetrics.getMetrics().stream()
        .map(InstanceInfo::getInstanceName)
        .distinct().count();
  }

  @VisibleForTesting
  PackingPlans.PackingPlan getSerializedPlan(PackingPlan packedPlan) {
    PackingPlanProtoSerializer serializer = new PackingPlanProtoSerializer();
    return serializer.toProto(packedPlan);
  }

  @Override
  public void close() {
  }
}
