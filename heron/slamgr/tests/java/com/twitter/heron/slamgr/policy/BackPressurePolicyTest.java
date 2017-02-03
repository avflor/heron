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

package com.twitter.heron.slamgr.policy;

import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.roundrobin.ResourceCompliantRRPacking;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.scheduler.client.SchedulerClientFactory;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.slamgr.utils.TestUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager;


public class BackPressurePolicyTest {

  private static final String STATE_MANAGER_CLASS = "STATE_MANAGER_CLASS";
  private IStateManager stateManager;
  private Config config;
  private TopologyAPI.Topology topology;

  /**
   * Basic setup before executing a test case
   */
  @Before
  public void setUp() throws Exception {
    this.topology = TestUtils.getTopology("ds");
  }

  @Test
  public void testPolicy() throws InterruptedException {

    Config config = Config.newBuilder()
        .put(Keys.repackingClass(), ResourceCompliantRRPacking.class.getName())
        .put(Keys.instanceCpu(), "1")
        .put(Keys.instanceRam(), 192L * Constants.MB)
        .put(Keys.instanceDisk(), 1024L * Constants.MB)
        .put(Keys.stateManagerRootPath(), "/home/avrilia/.herondata/repository/state/local")
        .put(Keys.stateManagerClass(), LocalFileSystemStateManager.class.getName())
        .build();

    stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);
    SchedulerStateManagerAdaptor adaptor =
        new SchedulerStateManagerAdaptor(stateManager, 5000);

    Config runtime = Config.newBuilder()
        .put(Keys.schedulerStateManagerAdaptor(), adaptor)
        .put(Keys.topologyName(), "ds")
        .build();

    ISchedulerClient schedulerClient = new SchedulerClientFactory(config, runtime)
        .getSchedulerClient();


    runtime = Config.newBuilder()
        .putAll(runtime)
        .put(Keys.schedulerClientInstance(), schedulerClient)
        .build();
    TrackerVisitor visitor = new TrackerVisitor();

    visitor.initialize(config, topology);

    BackPressurePolicy policy = new BackPressurePolicy();
    policy.initialize(config, runtime, topology, visitor);

    policy.execute();
  }
}
