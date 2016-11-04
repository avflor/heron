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

package com.twitter.heron.slamgr.detector;

import java.util.HashSet;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.IDetector;

/**
 * Detects the instances that have failed tuples.
 */
public class FailedTuplesDetector implements IDetector<FailedTuplesResult> {
  private SinkVisitor visitor;

  @Override
  public void initialize(Config config, SinkVisitor sVisitor) {
    this.visitor = sVisitor;
  }

  @Override
  public Diagnosis<FailedTuplesResult> detect(TopologyAPI.Topology topology) {
    Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric("__fail-count/default");
    Set<FailedTuplesResult> instanceInfo = new HashSet<FailedTuplesResult>();
    for (MetricsInfo metricsInfo : metricsResults) {
      String[] parts = metricsInfo.getName().split("_");
      instanceInfo.add(new FailedTuplesResult(parts[1], parts[3], metricsInfo.getValue()));
    }
    return new Diagnosis<FailedTuplesResult>(instanceInfo);
  }

  @Override
  public void close() {
  }

}


