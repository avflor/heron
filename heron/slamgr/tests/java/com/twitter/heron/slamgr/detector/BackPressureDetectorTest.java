//  Copyright 2016 Twitter. All rights reserved.
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

import com.google.common.util.concurrent.SettableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.slamgr.utils.TestUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
    TopologyUtils.class, ReflectionUtils.class, TopologyAPI.Topology.class})
public class BackPressureDetectorTest {


  private static final String STATE_MANAGER_CLASS = "STATE_MANAGER_CLASS";
  private IStateManager stateManager;
  private Config config;
  private TopologyAPI.Topology topology;
  private Config spyRuntime;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("ds");

    config = Config.newBuilder()
        .put(Keys.repackingClass(), ResourceCompliantRRPacking.class.getName())
        .put(Keys.instanceCpu(), "1")
        .put(Keys.instanceRam(), 192L * Constants.MB)
        .put(Keys.instanceDisk(), 1024L * Constants.MB)
        .build();

    spyRuntime = Mockito.spy(Config.newBuilder().build());

    ISchedulerClient schedulerClient = Mockito.mock(ISchedulerClient.class);
    when(spyRuntime.get(Keys.schedulerClientInstance())).thenReturn(schedulerClient);

    stateManager = mock(IStateManager.class);
    SettableFuture<PackingPlans.PackingPlan> future = TestUtils.getTestPacking(this.topology);
    when(stateManager.getPackingPlan(null, "ds")).thenReturn(future);
    when(spyRuntime.get(Keys.schedulerStateManagerAdaptor()))
        .thenReturn(new SchedulerStateManagerAdaptor(stateManager, 5000));

  }

  @Test
  public void testDetector() {

    TrackerVisitor visitor = new TrackerVisitor();
    visitor.initialize(config, topology);

    BackPressureDetector detector = new BackPressureDetector();
    detector.initialize(config, spyRuntime, visitor);

    Diagnosis<ComponentBottleneck> result = detector.detect(topology);
    Assert.assertEquals(1, result.getSummary().size());
  }
}