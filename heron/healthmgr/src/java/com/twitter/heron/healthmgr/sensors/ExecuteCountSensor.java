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

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;

import com.microsoft.dhalion.api.MetricsProvider;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.MetricsStats;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.common.TopologyProvider;

import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;

public class ExecuteCountSensor extends BaseSensor {
  private final MetricsProvider metricsProvider;
  //private Map<String, MetricsStats> executeCountStats;

  @Inject
  ExecuteCountSensor(TopologyProvider topologyProvider,
                     HealthPolicyConfig policyConfig,
                     MetricsProvider metricsProvider) {
    super(topologyProvider, policyConfig, METRIC_EXE_COUNT.text(),
        ExecuteCountSensor.class
            .getSimpleName());
    this.metricsProvider = metricsProvider;
    //this.executeCountStats = new HashMap<>();
  }

  @Override
  public Map<String, ComponentMetrics> fetchMetrics() {
    this.metrics = readMetrics(topologyProvider.getBoltNames());
    return this.metrics;
  }

  public Map<String, ComponentMetrics> readMetrics(String... boltNames) {
    Map<String, ComponentMetrics> processingRates = metricsProvider.getComponentMetrics
        (getMetricName(),
            getDuration(),
            boltNames);
    updateStats(processingRates);
    return processingRates;
  }

  private void updateStats(Map<String, ComponentMetrics> processingRates) {
    for (ComponentMetrics compMetrics : processingRates.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats currentStats = compStats.computeStats(MetricName.
          METRIC_EXE_COUNT.text());
      MetricsStats componentStats = this.stats.get(compMetrics.getComponentName());
      if (componentStats == null) {
        this.stats.put(compMetrics.getComponentName(), currentStats);
      } else {
        if (currentStats.getMetricMin() < componentStats.getMetricMin()) {
          componentStats.setMetricMin(currentStats.getMetricMin());
        }
        if (currentStats.getMetricMax() > componentStats.getMetricMax()) {
          componentStats.setMetricMax(currentStats.getMetricMax());
        }
        if (currentStats.getMetricAvg() > componentStats.getMetricAvg()) {
          componentStats.setMetricAvg(currentStats.getMetricAvg());
        }
      }
      this.stats.put(compMetrics.getComponentName(), componentStats);
    }
  }

}
