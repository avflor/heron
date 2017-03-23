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

package com.twitter.heron.healthmgr.diagnoser;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.healthmgr.services.DiagnoserService;
import com.twitter.heron.healthmgr.services.SymptomDetectorService;
import com.twitter.heron.healthmgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.healthmgr.utils.TestUtils;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.packing.PackingPlan;


public class OverProvisioningDiagnoserTest {
  private Config config;
  private Config runtime;

  private TopologyAPI.Topology topology;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("ex");
    config = Config.newBuilder()
        .put(Key.REPACKING_CLASS, ResourceCompliantRRPacking.class.getName())
        .put(Key.INSTANCE_CPU, "1")
        .put(Key.INSTANCE_RAM, ByteAmount.fromMegabytes(192).asBytes())
        .put(Key.INSTANCE_DISK, ByteAmount.fromGigabytes(1).asBytes())
        .put(Key.TRACKER_URL, "http://localhost:8888")
        .put(Key.CLUSTER, "local")
        .build();

    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    PackingPlan packingPlan = packing.pack();


    TrackerVisitor visitor = new TrackerVisitor();
    runtime = Config.newBuilder()
        .put(Key.TOPOLOGY_DEFINITION, topology)
        .put(Key.TRACKER_URL, "http://localhost:8888")
        .put(Key.PACKING_PLAN, packingPlan)
        .put(Key.METRICS_READER_INSTANCE, visitor)
        .put(Key.HEALTH_MGR_DIAGNOSER_SERVICE, new DiagnoserService())
        .put(Key.HEALTH_MGR_SYMPTOM_DETECTOR_SERVICE, new SymptomDetectorService())
        .build();
    visitor.initialize(config, runtime);
  }

  @Test
  public void testDiagnoser() {

    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(config, runtime);

    OverprovisioningDiagnoser diagnoser = new OverprovisioningDiagnoser();
    diagnoser.initialize(config, runtime);
    diagnoser.setPacketThreshold(500);
    Diagnosis<ComponentSymptom> result = diagnoser.diagnose(topology);
    Assert.assertEquals(1, result.getSummary().size());
  }
}
