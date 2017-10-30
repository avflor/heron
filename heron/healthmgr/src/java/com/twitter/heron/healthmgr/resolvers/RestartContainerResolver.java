// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.healthmgr.resolvers;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import com.twitter.heron.proto.scheduler.Scheduler.RestartTopologyRequest;
import com.twitter.heron.scheduler.client.ISchedulerClient;

import static com.twitter.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;
import static com.twitter.heron.healthmgr.detectors.BackPressureDetector.CONF_NOISE_FILTER;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.SYMPTOM_SLOW_INSTANCE;

public class RestartContainerResolver implements IResolver {
  private static final Logger LOG = Logger.getLogger(RestartContainerResolver.class.getName());

  private final EventManager eventManager;
  private final String topologyName;
  private final ISchedulerClient schedulerClient;
  private final int noiseFilterMillis;

  @Inject
  public RestartContainerResolver(@Named(CONF_TOPOLOGY_NAME) String topologyName,
                                  EventManager eventManager, ISchedulerClient schedulerClient,
                                  HealthPolicyConfig policyConfig) {
    this.topologyName = topologyName;
    this.eventManager = eventManager;
    this.schedulerClient = schedulerClient;
    this.noiseFilterMillis = (int) policyConfig.getConfig(CONF_NOISE_FILTER, 20);
  }

  @Override
  public List<Action> resolve(List<Diagnosis> diagnosis) {
    List<Action> actions = new ArrayList<>();

    for (Diagnosis diagnoses : diagnosis) {
      Symptom bpSymptom = diagnoses.getSymptoms().get(SYMPTOM_SLOW_INSTANCE.text());
      if (bpSymptom == null || bpSymptom.getComponentMetrics().getMetrics().isEmpty()) {
        // nothing to fix as there is no back pressure
        continue;
      }

      ComponentMetrics compMetrics = bpSymptom.getComponentMetrics();
      if (compMetrics.getComponentNames().size() > 1) {
        throw new UnsupportedOperationException("Multiple components with back pressure symptom");
      }

      // want to know which stmgr has backpressure
      String stmgrId = null;
      for (InstanceMetrics im : compMetrics.getMetrics()) {
        if (im.getValueSum() > noiseFilterMillis) {
          String instanceId = im.getInstanceName();
          int fromIndex = instanceId.indexOf('_') + 1;
          int toIndex = instanceId.indexOf('_', fromIndex);
          stmgrId = instanceId.substring(fromIndex, toIndex);
          break;
        }
      }
      LOG.info("Restarting container: " + stmgrId);
      boolean b = schedulerClient.restartTopology(
          RestartTopologyRequest.newBuilder()
              .setContainerIndex(Integer.valueOf(stmgrId))
              .setTopologyName(topologyName)
              .build());
      LOG.info("Restarted container result: " + b);

      ContainerRestart action = new ContainerRestart();
      LOG.info("Broadcasting container restart event");
      eventManager.onEvent(action);

      actions.add(action);
      return actions;
    }

    return actions;
  }

  @Override
  public void close() {
  }
}
