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


package com.twitter.heron.healthmgr.sensors;

import java.util.Collection;

import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;

public class BufferSizeSensor extends BaseSensor {
  private final MetricsProvider metricsProvider;
  private final PackingPlanProvider packingPlanProvider;
  private final TopologyProvider topologyProvider;

  @Inject
  public BufferSizeSensor(HealthPolicyConfig policyConfig,
                          PackingPlanProvider packingPlanProvider,
                          TopologyProvider topologyProvider,
                          MetricsProvider metricsProvider) {
    super(policyConfig, METRIC_BUFFER_SIZE.text(), BufferSizeSensor.class.getSimpleName());
    this.packingPlanProvider = packingPlanProvider;
    this.topologyProvider = topologyProvider;
    this.metricsProvider = metricsProvider;
  }

  /**
   * The buffer size as provided by tracker
   *
   * @return buffer size
   */
  @Override
  public ComponentMetrics fetchMetrics() {
    metrics = new ComponentMetrics();
    String[] boltComponents = topologyProvider.getBoltNames();
    for (String boltComponent : boltComponents) {
      String[] boltInstanceNames = packingPlanProvider.getBoltInstanceNames(boltComponent);
      for (String boltInstanceName : boltInstanceNames) {
        String metric = getMetricName() + boltInstanceName + MetricName.METRIC_BUFFER_SIZE_SUFFIX;

        ComponentMetrics stmgrResult = metricsProvider.getComponentMetrics(
            metric, getDuration(), COMPONENT_STMGR);

        Collection<InstanceMetrics> streamManagerResult =
            stmgrResult.filterByComponent(COMPONENT_STMGR).getMetrics();
        if (streamManagerResult.isEmpty()) {
          continue;
        }

        // since a bolt instance belongs to one stream manager, expect just one metrics
        // manager instance in the result
        Double stmgrInstanceResult = 0.0;
        for (InstanceMetrics iMetrics : streamManagerResult) {
          Double val = iMetrics.getValueSum();
          if (val != null) {
            stmgrInstanceResult += val;
          }
        }

        InstanceMetrics boltInstanceMetric =
            new InstanceMetrics(boltComponent, boltInstanceName, getMetricName());
        boltInstanceMetric.addValue(stmgrInstanceResult);
        metrics.add(boltInstanceMetric);
      }
    }

    return metrics;
  }
}
