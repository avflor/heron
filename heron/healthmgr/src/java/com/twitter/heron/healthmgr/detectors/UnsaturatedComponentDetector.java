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


package com.twitter.heron.healthmgr.detectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.inject.Inject;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.MetricsStats;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.sensors.BackPressureSensor;
import com.twitter.heron.healthmgr.sensors.BaseSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_HIGHCONF;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_LOWCONF;


public class UnsaturatedComponentDetector extends BaseDetector {
  static final String CONF_NOISE_FILTER = "BackPressureDetector.noiseFilterMillis";
  private static final Logger LOG = Logger.getLogger(UnsaturatedComponentDetector.class.getName());
  private final ExecuteCountSensor executeCountSensor;
  private final BackPressureSensor backPressureSensor;
  private final int noiseFilterMillis;

  @Inject
  UnsaturatedComponentDetector(ExecuteCountSensor executeCountSensor,
                               BackPressureSensor backPressureSensor,
                               HealthPolicyConfig policyConfig) {
    this.executeCountSensor = executeCountSensor;
    this.backPressureSensor = backPressureSensor;
    noiseFilterMillis = (int) policyConfig.getConfig(CONF_NOISE_FILTER, 20);
  }

  /**
   * Detects all components whose processing rate is lower than what has been observed before
   * (unsaturated components)
   *
   * @return A collection of all unsaturated components.
   */
  @Override
  public List<Symptom> detect() {
    ArrayList<Symptom> result = new ArrayList<>();

    Map<String, ComponentMetrics> processingRates = executeCountSensor.get();
    Map<String, ComponentMetrics> backpressureMetrics = backPressureSensor.get();
    for (ComponentMetrics compMetrics : backpressureMetrics.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      compStats.computeBpStats();
      if (compStats.getTotalBackpressure() > noiseFilterMillis) {
        return result;
      }
    }

    for (ComponentMetrics compMetrics : processingRates.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats currentStats = compStats.computeStats(BaseSensor.MetricName.
          METRIC_EXE_COUNT.text());
      MetricsStats processingRateStats = executeCountSensor.getStats(compMetrics.getComponentName());
      if (processingRateStats == null) {
        return result;
      }
      if (currentStats.getMetricAvg() <= 0.8 * processingRateStats.getMetricAvg()) {
        LOG.info(String.format("Detected unsaturated component with high confidence %s: current "
                + "average processing " + "rate is %f, previous observed maximum average rate is "
            + "%f", compMetrics.getComponentName(), currentStats.getMetricAvg(),
            processingRateStats.getMetricAvg()));
        result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_HIGHCONF.text(), compMetrics,
            processingRateStats));
      } else if (currentStats.getMetricAvg() > 0.8 * processingRateStats.getMetricAvg() &&
          currentStats.getMetricAvg() <= processingRateStats.getMetricAvg()) {
        LOG.info(String.format("Detected unsaturated component with low confidence %s: current "
                + "average processing " + "rate is %f, previous observed maximum average rate is "
            + "%f",
            compMetrics.getComponentName(), currentStats.getMetricAvg(),
            processingRateStats.getMetricAvg()));
        result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_LOWCONF.text(), compMetrics,
            processingRateStats));
      }

    }
    return result;
  }
}
