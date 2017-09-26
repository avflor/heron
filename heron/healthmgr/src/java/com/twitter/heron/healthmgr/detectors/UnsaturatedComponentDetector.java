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

import com.google.common.base.Optional;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.MetricsStats;
import com.microsoft.dhalion.metrics.StatsCollector;

import com.twitter.heron.healthmgr.common.ComponentMetricsHelper;
import com.twitter.heron.healthmgr.sensors.BaseSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_HIGHCONF;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_LOWCONF;


public class UnsaturatedComponentDetector extends BaseDetector {

  private static final Logger LOG = Logger.getLogger(UnsaturatedComponentDetector.class.getName());
  private final ExecuteCountSensor executeCountSensor;
  private StatsCollector statsCollector;

  @Inject
  UnsaturatedComponentDetector(ExecuteCountSensor executeCountSensor,
                               StatsCollector statsCollector) {
    this.executeCountSensor = executeCountSensor;
    this.statsCollector = statsCollector;
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
    for (ComponentMetrics compMetrics : processingRates.values()) {
      ComponentMetricsHelper compStats = new ComponentMetricsHelper(compMetrics);
      MetricsStats stats = compStats.computeMinMaxStats(BaseSensor.MetricName.
          METRIC_EXE_COUNT.text());
      Optional<Double> maxProcessingRateObserved = this.statsCollector.getMetricData(BaseSensor
          .MetricName.METRIC_EXE_COUNT.text(), compMetrics.getName());
      Optional<Double> backpressureObserved = this.statsCollector.getMetricData(BaseSensor
          .MetricName.METRIC_BACK_PRESSURE.text(), compMetrics.getName());

      if (!maxProcessingRateObserved.isPresent() || !backpressureObserved.isPresent()) {
        return result;
      }

      if (backpressureObserved.get() > 0 && stats.getMetricMax() < maxProcessingRateObserved.get
          ()) {
        LOG.info(String.format("Detected unsaturated component with high confidence %s: current "
                + "maximum processing " + "rate is %f, previous observed maximum rate is %f",
            compMetrics.getName(), stats.getMetricMax(), maxProcessingRateObserved.get()));

        result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_HIGHCONF.text(), compMetrics));
      } else if (backpressureObserved.get() <= 0) {
        if (stats.getMetricMax() <= 0.8 * maxProcessingRateObserved.get()) {
          LOG.info(String.format("Detected unsaturated component with high confidence %s: current "
                  + "maximum processing " + "rate is %f, previous observed maximum rate is %f",
              compMetrics.getName(), stats.getMetricMax(), maxProcessingRateObserved.get()));
          result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_HIGHCONF.text(), compMetrics));
        } else if (stats.getMetricMax() > 0.8 * maxProcessingRateObserved.get()
            && stats.getMetricMax() <= maxProcessingRateObserved.get()) {
          LOG.info(String.format("Detected unsaturated component with low confidence %s: current "
                  + "maximum processing " + "rate is %f, previous observed maximum rate is %f",
              compMetrics.getName(), stats.getMetricMax(), maxProcessingRateObserved.get()));
          result.add(new Symptom(SYMPTOM_UNSATURATEDCOMP_LOWCONF.text(), compMetrics));
        }
      }
    }
    return result;
  }
}
