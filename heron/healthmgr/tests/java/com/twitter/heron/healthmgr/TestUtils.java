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

package com.twitter.heron.healthmgr;

import java.util.ArrayList;
import java.util.List;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName;
import com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName;

import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_GROWING_WAIT_Q;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_LARGE_WAIT_Q;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_PROCESSING_RATE_SKEW;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_SMALL_WAIT_Q;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_HIGHCONF;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_LOWCONF;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_WAIT_Q_DISPARITY;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;


public final class TestUtils {

  private TestUtils() {
  }

  public static List<Symptom> createBpSymptomList(int... bpValues) {
    return createListFromSymptom(createBPSymptom(bpValues));
  }

  public static Symptom createExeCountSymptom(int... exeCounts) {
    return createSymptom(SYMPTOM_PROCESSING_RATE_SKEW, METRIC_EXE_COUNT, exeCounts);
  }

  public static Symptom createWaitQueueDisparitySymptom(int... bufferSizes) {
    return createSymptom(SYMPTOM_WAIT_Q_DISPARITY, METRIC_BUFFER_SIZE, bufferSizes);
  }


  public static Symptom createLargeWaitQSymptom(int... bufferSizes) {
    return createSymptom(SYMPTOM_LARGE_WAIT_Q, METRIC_BUFFER_SIZE, bufferSizes);
  }

  public static Symptom createSmallWaitQSymptom(int... bufferSizes) {
    return createSymptom(SYMPTOM_SMALL_WAIT_Q, METRIC_BUFFER_SIZE, bufferSizes);
  }

  public static Symptom createHighConfUnsaturatedComponentSymptom(int... exeCounts) {
    return createSymptom(SYMPTOM_UNSATURATEDCOMP_HIGHCONF, METRIC_EXE_COUNT, exeCounts);
  }

  public static Symptom createLowConfUnsaturatedComponentSymptom(int... exeCounts) {
    return createSymptom(SYMPTOM_UNSATURATEDCOMP_LOWCONF, METRIC_EXE_COUNT, exeCounts);
  }

  public static Symptom createGrowingWaitingQueueSymptom(int... bufferSizes) {
    return createSymptom(SYMPTOM_GROWING_WAIT_Q, METRIC_BUFFER_SIZE, bufferSizes);
  }

  public static Symptom createBPSymptom(int... bpValues) {
    return createSymptom(SYMPTOM_BACK_PRESSURE, METRIC_BACK_PRESSURE, bpValues);
  }

  private static Symptom createSymptom(SymptomName symptom, MetricName metric, int... vals) {
    ComponentMetrics metrics = new ComponentMetrics();
    for (int i = 0; i < vals.length; i++) {
      metrics.addMetric("bolt", "container_1_bolt_" + i, metric.text(), vals[i]);
    }
    return new Symptom(symptom.text(), metrics);
  }

  private static List<Symptom> createListFromSymptom(Symptom symptom) {
    List<Symptom> symptoms = new ArrayList<>();
    symptoms.add(symptom);
    return symptoms;
  }
}
