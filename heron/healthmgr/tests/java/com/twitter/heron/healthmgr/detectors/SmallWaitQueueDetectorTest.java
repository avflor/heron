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

import java.util.List;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.BufferSizeSensor;

import static com.twitter.heron.healthmgr.detectors.SmallWaitQueueDetector.SMALL_WAIT_QUEUE_SIZE_LIMIT;
import static com.twitter.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BUFFER_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SmallWaitQueueDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);

    when(config.getConfig(SMALL_WAIT_QUEUE_SIZE_LIMIT, 5.0)).thenReturn(5.0);
    ComponentMetrics compMetrics = new ComponentMetrics();
    compMetrics.addMetric("bolt", "i1", METRIC_BUFFER_SIZE.text(), 5);

    BufferSizeSensor sensor = mock(BufferSizeSensor.class);
    when(sensor.getMetrics()).thenReturn(compMetrics);

    SmallWaitQueueDetector detector = new SmallWaitQueueDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    assertEquals(1, symptoms.size());

    compMetrics = new ComponentMetrics();
    compMetrics.addMetric("bolt", "i1", METRIC_BUFFER_SIZE.text(), 6);

    sensor = mock(BufferSizeSensor.class);
    when(sensor.getMetrics()).thenReturn(compMetrics);

    detector = new SmallWaitQueueDetector(sensor, config);
    symptoms = detector.detect();

    assertEquals(0, symptoms.size());
  }
}
