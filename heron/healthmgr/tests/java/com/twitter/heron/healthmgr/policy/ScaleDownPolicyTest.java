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

package com.twitter.heron.healthmgr.policy;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.healthmgr.services.DiagnoserService;
import com.twitter.heron.healthmgr.services.ResolverService;
import com.twitter.heron.healthmgr.services.SymptomDetectorService;
import com.twitter.heron.healthmgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.healthmgr.utils.TestUtils;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager;


public class ScaleDownPolicyTest {

  private IStateManager stateManager;
  private TopologyAPI.Topology topology;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("ex");
  }

  @Test
  public void testPolicy() throws InterruptedException {

    SymptomDetectorService sd = new SymptomDetectorService();
    DiagnoserService ds = new DiagnoserService();
    ResolverService rs = new ResolverService();
    Config config = Config.newBuilder()
        .put(Key.REPACKING_CLASS, ResourceCompliantRRPacking.class.getName())
        .put(Key.INSTANCE_CPU, "1")
        .put(Key.INSTANCE_RAM, ByteAmount.fromMegabytes(192).asBytes())
        .put(Key.INSTANCE_DISK, ByteAmount.fromGigabytes(1).asBytes())
        .put(Key.STATEMGR_ROOT_PATH, "/home/avrilia/.herondata/repository/state/local")
        .put(Key.STATE_MANAGER_CLASS, LocalFileSystemStateManager.class.getName())
        .put(Key.TOPOLOGY_NAME, "ex")
        .put(Key.CLUSTER, "local")
        .put(Key.TRACKER_URL, "http://localhost:8888")
        .put(Key.SCHEDULER_IS_SERVICE, true)
        .build();

    stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);
    SchedulerStateManagerAdaptor adaptor =
        new SchedulerStateManagerAdaptor(stateManager, 5000);

    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    PackingPlan packingPlan = packing.pack();

    Config runtime = Config.newBuilder()
        .put(Key.SCHEDULER_STATE_MANAGER_ADAPTOR, adaptor)
        .put(Key.TOPOLOGY_NAME, "ex")
        .put(Key.TRACKER_URL, "http://localhost:8888")
        .put(Key.HEALTH_MGR_SYMPTOM_DETECTOR_SERVICE, sd)
        .put(Key.HEALTH_MGR_DIAGNOSER_SERVICE, ds)
        .put(Key.HEALTH_MGR_RESOLVER_SERVICE, rs)
        .put(Key.PACKING_PLAN, packingPlan)
        .build();

    ISchedulerClient schedulerClient = new SchedulerClientFactory(config, runtime)
        .getSchedulerClient();

    TrackerVisitor visitor = new TrackerVisitor();

    runtime = Config.newBuilder()
        .putAll(runtime)
        .put(Key.SCHEDULER_CLIENT_INSTANCE, schedulerClient)
        .put(Key.METRICS_READER_INSTANCE, visitor)
        .put(Key.TOPOLOGY_DEFINITION, this.topology)
        .build();

    visitor.initialize(config, runtime);

    ScaleDownPolicy policy = new ScaleDownPolicy();
    policy.initialize(config, runtime);
    policy.setPacketsThreshold(200);
    policy.execute();

    TimeUnit.MINUTES.sleep(2);
    //for(int i = 0 ; i < 100; i++) {
    policy.evaluate();
    //}
  }
}
