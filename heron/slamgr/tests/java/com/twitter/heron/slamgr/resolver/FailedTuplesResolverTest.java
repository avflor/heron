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

package com.twitter.heron.slamgr.resolver;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.detector.FailedTuplesDetector;
import com.twitter.heron.slamgr.detector.FailedTuplesResult;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.InstanceBottleneck;
import com.twitter.heron.spi.utils.TopologyTests;

public class FailedTuplesResolverTest {

  private static final String BOLT_NAME = "exclaim1";
  private static final String SPOUT_NAME = "word";

  private TopologyAPI.Topology getTopology(
      int spoutParallelism, int boltParallelism,
      com.twitter.heron.api.Config topologyConfig) {
    return TopologyTests.createTopology("ExclamationTopology", topologyConfig, SPOUT_NAME,
        BOLT_NAME, spoutParallelism, boltParallelism);
  }

  @Test
  public void testResolver() {

    TopologyAPI.Topology topology = getTopology(1, 2, new com.twitter.heron.api.Config());
    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(null, topology);

    FailedTuplesDetector detector = new FailedTuplesDetector();
    detector.initialize(null, visitor);

    Diagnosis<InstanceBottleneck> result = detector.detect(topology);
    Assert.assertEquals(2, result.getSummary().size());

    FailedTuplesResolver resolver = new FailedTuplesResolver();
    resolver.initialize(null, null);

    resolver.resolve(result, topology);

  }
}
