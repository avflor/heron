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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.detectors.ProcessingRateSkewDetector.CONF_SKEW_RATIO;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessingRateSkewDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    ComponentMetrics topologyMetrics = new ComponentMetrics();
    topologyMetrics.addMetric("bolt","i1", METRIC_EXE_COUNT.text(), 1000);
    topologyMetrics.addMetric("bolt","i2", METRIC_EXE_COUNT.text(), 200);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.getMetrics()).thenReturn(topologyMetrics);
    when(sensor.getMetricName()).thenReturn(METRIC_EXE_COUNT.text());

    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(1, symptoms.size());

    topologyMetrics = new ComponentMetrics();
    topologyMetrics.addMetric("bolt","i1", METRIC_EXE_COUNT.text(), 1000);
    topologyMetrics.addMetric("bolt","i2", METRIC_EXE_COUNT.text(), 500);

    sensor = mock(ExecuteCountSensor.class);
    when(sensor.getMetrics()).thenReturn(topologyMetrics);
    when(sensor.getMetricName()).thenReturn(METRIC_EXE_COUNT.text());

    detector = new ProcessingRateSkewDetector(sensor, config);
    symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }

  @Test
  public void testReturnsMultipleComponents() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_SKEW_RATIO, 1.5)).thenReturn(2.5);

    ComponentMetrics topologyMetrics = new ComponentMetrics();
    topologyMetrics.addMetric("bolt-1","i1", METRIC_EXE_COUNT.text(), 1000);
    topologyMetrics.addMetric("bolt-1","i2", METRIC_EXE_COUNT.text(), 200);
    topologyMetrics.addMetric("bolt-2","i1", METRIC_EXE_COUNT.text(), 1000);
    topologyMetrics.addMetric("bolt-2","i2", METRIC_EXE_COUNT.text(), 200);
    topologyMetrics.addMetric("bolt-3","i1", METRIC_EXE_COUNT.text(), 1000);
    topologyMetrics.addMetric("bolt-3","i2", METRIC_EXE_COUNT.text(), 500);

    ExecuteCountSensor sensor = mock(ExecuteCountSensor.class);
    when(sensor.getMetrics()).thenReturn(topologyMetrics);
    when(sensor.getMetricName()).thenReturn(METRIC_EXE_COUNT.text());

    ProcessingRateSkewDetector detector = new ProcessingRateSkewDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(2, symptoms.size());
    Set<String> comps = symptoms.stream().
        flatMap(symptom -> symptom.getComponentMetrics().getComponentNames().stream())
        .collect(Collectors.toSet());

    assertEquals(2, comps.size());
    assertTrue(comps.contains("bolt-1"));
    assertTrue(comps.contains("bolt-2"));
  }
}
