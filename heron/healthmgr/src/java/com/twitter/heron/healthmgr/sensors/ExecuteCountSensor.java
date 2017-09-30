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
  private final TopologyProvider topologyProvider;
  private final MetricsProvider metricsProvider;
  private Map<String, MetricsStats> executeCountStats;

  @Inject
  ExecuteCountSensor(TopologyProvider topologyProvider,
                     HealthPolicyConfig policyConfig,
                     MetricsProvider metricsProvider) {
    super(policyConfig, METRIC_EXE_COUNT.text(), ExecuteCountSensor.class.getSimpleName());
    this.topologyProvider = topologyProvider;
    this.metricsProvider = metricsProvider;
    this.executeCountStats = new HashMap<>();
  }

  public Map<String, ComponentMetrics> get() {
    String[] boltNames = topologyProvider.getBoltNames();
    return get(boltNames);
  }

  public Map<String, ComponentMetrics> get(String... boltNames) {
    Map<String, ComponentMetrics> processingRates =  metricsProvider.getComponentMetrics
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
      MetricsStats componentStats = executeCountStats.get(compMetrics.getComponentName());
      if(componentStats == null){
        executeCountStats.put(compMetrics.getComponentName(), currentStats);
      }
      else{
        if(currentStats.getMetricMin() < componentStats.getMetricMin()){
          componentStats.setMetricMin(currentStats.getMetricMin());
        }
        if(currentStats.getMetricMax() > componentStats.getMetricMax()){
          componentStats.setMetricMax(currentStats.getMetricMax());
        }
        if(currentStats.getMetricAvg() > componentStats.getMetricAvg()){
          componentStats.setMetricAvg(currentStats.getMetricAvg());
        }
      }
      executeCountStats.put(compMetrics.getComponentName(), componentStats);
    }
  }

  @Override
  public MetricsStats getStats(String component){

    if(executeCountStats.get(component) == null){
      this.get(component);
    }
    return executeCountStats.get(component);
  }

}
