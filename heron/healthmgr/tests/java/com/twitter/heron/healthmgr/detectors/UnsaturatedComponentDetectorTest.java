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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.MetricsStats;

import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.BackPressureSensor;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.detectors.BackPressureDetector.CONF_NOISE_FILTER;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_HIGHCONF;
import static com.twitter.heron.healthmgr.detectors.BaseDetector.SymptomName.SYMPTOM_UNSATURATEDCOMP_LOWCONF;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_EXE_COUNT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnsaturatedComponentDetectorTest {

  @Test
  public void testHighConfUnsaturatedComponent() {

    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, 20)).thenReturn(50);

    ComponentMetrics exMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT.text(), 5);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", exMetrics);

    ExecuteCountSensor exsensor = mock(ExecuteCountSensor.class);
    when(exsensor.get()).thenReturn(topologyMetrics);
    when(exsensor.getStats("bolt")).thenReturn(new MetricsStats(2, 10, 10));

    ComponentMetrics bpMetrics =
        new ComponentMetrics("bolt", "i1", METRIC_BACK_PRESSURE.text(), 0);
    Map<String, ComponentMetrics> topologybpMetrics = new HashMap<>();
    topologybpMetrics.put("bolt", bpMetrics);

    BackPressureSensor bpsensor = mock(BackPressureSensor.class);
    when(bpsensor.get()).thenReturn(topologybpMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(exsensor, bpsensor,
        config);
    List<Symptom> symptoms = detector.detect();
    assertEquals(SYMPTOM_UNSATURATEDCOMP_HIGHCONF.text(), symptoms.get(0).getSymptomName());
    assertEquals(1, symptoms.size());
  }

  @Test
  public void testLowConfUnsaturatedComponent() {

    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, 20)).thenReturn(50);

    ComponentMetrics exMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT.text(), 5);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", exMetrics);

    ExecuteCountSensor exsensor = mock(ExecuteCountSensor.class);
    when(exsensor.get()).thenReturn(topologyMetrics);
    when(exsensor.getStats("bolt")).thenReturn(new MetricsStats(2, 6, 6));

    ComponentMetrics bpMetrics =
        new ComponentMetrics("bolt", "i1", METRIC_BACK_PRESSURE.text(), 0);
    Map<String, ComponentMetrics> topologybpMetrics = new HashMap<>();
    topologybpMetrics.put("bolt", bpMetrics);

    BackPressureSensor bpsensor = mock(BackPressureSensor.class);
    when(bpsensor.get()).thenReturn(topologybpMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(exsensor, bpsensor,
        config);
    List<Symptom> symptoms = detector.detect();
    assertEquals(SYMPTOM_UNSATURATEDCOMP_LOWCONF.text(), symptoms.get(0).getSymptomName());
    assertEquals(1, symptoms.size());
  }

  @Test
  public void testNegativeUnsaturatedComponentwithbackpressure() {

    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, 20)).thenReturn(50);

    ComponentMetrics compMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT.text(), 5);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor exsensor = mock(ExecuteCountSensor.class);
    when(exsensor.get()).thenReturn(topologyMetrics);
    when(exsensor.getStats("bolt")).thenReturn(new MetricsStats(2, 6, 6));

    ComponentMetrics bpMetrics =
        new ComponentMetrics("bolt", "i1", METRIC_BACK_PRESSURE.text(), 100);
    Map<String, ComponentMetrics> topologybpMetrics = new HashMap<>();
    topologybpMetrics.put("bolt", bpMetrics);

    BackPressureSensor bpsensor = mock(BackPressureSensor.class);
    when(bpsensor.get()).thenReturn(topologybpMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(exsensor,
        bpsensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }


  @Test
  public void testNegativeUnsaturatedComponentNoBackpressure() {

    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, 20)).thenReturn(50);

    ComponentMetrics compMetrics = new ComponentMetrics("bolt", "i1", METRIC_EXE_COUNT.text(), 5);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    ExecuteCountSensor exsensor = mock(ExecuteCountSensor.class);
    when(exsensor.get()).thenReturn(topologyMetrics);
    when(exsensor.getStats("bolt")).thenReturn(new MetricsStats(1, 4, 4));

    ComponentMetrics bpMetrics =
        new ComponentMetrics("bolt", "i1", METRIC_BACK_PRESSURE.text(), 0);
    Map<String, ComponentMetrics> topologybpMetrics = new HashMap<>();
    topologybpMetrics.put("bolt", bpMetrics);

    BackPressureSensor bpsensor = mock(BackPressureSensor.class);
    when(bpsensor.get()).thenReturn(topologybpMetrics);

    UnsaturatedComponentDetector detector = new UnsaturatedComponentDetector(exsensor, bpsensor,
        config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }

}
