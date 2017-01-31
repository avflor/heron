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
package com.twitter.heron.slamgr.detector;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoDeserializer;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.IDetector;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.Runtime;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BackPressureDetector implements IDetector<ComponentBottleneck> {

  private final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private SinkVisitor visitor;
  private SchedulerStateManagerAdaptor adaptor;

  @Override
  public boolean initialize(Config config, Config runtime, SinkVisitor sVisitor) {
    this.visitor = sVisitor;
    this.adaptor = Runtime.schedulerStateManagerAdaptor(runtime);
    return true;
  }

  public String getBackPressureMetric(PackingPlan.ContainerPlan containerPlan,
                                      PackingPlan.InstancePlan instancePlan) {
    String name = "container_" + containerPlan.getId()
        + "_" + instancePlan.getComponentName()
        + "_" + instancePlan.getTaskId();
    //System.out.println(BACKPRESSURE_METRIC +"/" + name);
    Collection<MetricsInfo> metricsResults =
        this.visitor.getNextMetric(BACKPRESSURE_METRIC + "/" + name, "__stmgr__");
    if (metricsResults.size() > 1) {
      throw new IllegalStateException(
          String.format("More than one metric (%d) received for %s", metricsResults.size(), BACKPRESSURE_METRIC));
    }

    if (metricsResults.isEmpty()) {
      return null;
    }

    return metricsResults.iterator().next().getValue();
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology)
      throws RuntimeException {

    PackingPlan packingPlan = getPackingPlan(topology);
    HashMap<String, ComponentBottleneck> results = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
        String metricValue = getBackPressureMetric(containerPlan, instancePlan);
        if (metricValue == null) {
          continue;
        }
        MetricsInfo metric = new MetricsInfo(BACKPRESSURE_METRIC, metricValue);
        ComponentBottleneck currentBottleneck;
        if (!results.containsKey(instancePlan.getComponentName())) {
          currentBottleneck = new ComponentBottleneck(instancePlan.getComponentName());
        } else {
          currentBottleneck = results.get(instancePlan.getComponentName());
        }
        Set<MetricsInfo> metrics = new HashSet<>();
        metrics.add(metric);
        currentBottleneck.add(containerPlan.getId(),
            instancePlan.getTaskId(), metrics);
        results.put(instancePlan.getComponentName(), currentBottleneck);
      }
    }
    Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();
    for (ComponentBottleneck bottleneck : results.values()) {
      if (bottleneck.containsNonZero(BACKPRESSURE_METRIC)) {
        System.out.println("bottleneck name " + bottleneck.getComponentName().toString());
        bottlenecks.add(bottleneck);
      }
    }
    return new Diagnosis<ComponentBottleneck>(bottlenecks);
  }

  private PackingPlan getPackingPlan(TopologyAPI.Topology topology) {
    // get a packed plan and schedule it
    PackingPlans.PackingPlan serializedPackingPlan = adaptor.getPackingPlan(topology.getName());
    if (serializedPackingPlan == null) {
      throw new RuntimeException(String.format("Failed to fetch PackingPlan for topology: %s " +
          "from the state manager", topology.getName()));
    }
    LOG.log(Level.INFO, "Packing plan fetched from state: {0}", serializedPackingPlan);
    PackingPlan packedPlan = new PackingPlanProtoDeserializer().fromProto(serializedPackingPlan);
    return packedPlan;
  }

  @Override
  public void close() {
  }
}


