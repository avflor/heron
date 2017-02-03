//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License

package com.twitter.heron.slamgr.detector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.utils.SLAManagerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.IDetector;
import com.twitter.heron.spi.slamgr.ThresholdBasedDetector;

/**
 * Detects the instances that have data skew.
 */
public class ReportingDetector implements IDetector<ComponentBottleneck> {

  private SinkVisitor visitor;
  private String metric;

  public ReportingDetector(String metric) {
    this.metric = metric;
  }

  @Override
  public boolean initialize(Config config, Config runtime, SinkVisitor sVisitor) {
    this.visitor = sVisitor;
    return true;
  }

  @Override
  public Diagnosis<ComponentBottleneck> detect(TopologyAPI.Topology topology) {
    List<TopologyAPI.Bolt> bolts = topology.getBoltsList();
    String[] boltNames = new String[bolts.size()];

    Set<ComponentBottleneck> bottlenecks = new HashSet<ComponentBottleneck>();

    for (int i = 0; i < boltNames.length; i++) {
      String component = bolts.get(i).getComp().getName();
      Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric(metric,
          component);

      ComponentBottleneck currentBottleneck;
      currentBottleneck = new ComponentBottleneck(component);
      for (MetricsInfo metricsInfo : metricsResults) {
        SLAManagerUtils.updateComponentBottleneck(currentBottleneck, metric,
            metricsInfo);
      }
      bottlenecks.add(currentBottleneck);
    }
    System.out.println(bottlenecks.toString());
    return new Diagnosis<ComponentBottleneck>(bottlenecks);
  }

  @Override
  public void close() {
  }

}


