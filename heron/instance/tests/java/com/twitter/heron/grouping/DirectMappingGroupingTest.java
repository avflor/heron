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

package com.twitter.heron.grouping;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.instance.Slave;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.TestBolt;
import com.twitter.heron.resource.TestSpout;
import com.twitter.heron.resource.UnitTestHelper;

public class DirectMappingGroupingTest {
  private static final String SPOUT_INSTANCE_ID = "spout-id";
  private static final String CUSTOM_GROUPING_INFO = "custom-grouping-info-in-prepare";

  private WakeableLooper testLooper;
  private SlaveLooper slaveLooper;
  private PhysicalPlans.PhysicalPlan physicalPlan;
  private Communicator<HeronTuples.HeronTupleSet> outStreamQueue;
  private Communicator<HeronTuples.HeronTupleSet> inStreamQueue;
  private Communicator<InstanceControlMsg> inControlQueue;
  private ExecutorService threadsPool;
  private Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut;
  private volatile int tupleReceived;
  private volatile StringBuilder customGroupingInfoInPrepare;
  private Slave slave;

  @Before
  public void before() throws Exception {
    UnitTestHelper.addSystemConfigToSingleton();

    tupleReceived = 0;
    customGroupingInfoInPrepare = new StringBuilder();

    testLooper = new SlaveLooper();
    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, testLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(testLooper, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<InstanceControlMsg>(testLooper, slaveLooper);

    slaveMetricsOut =
        new Communicator<Metrics.MetricPublisherPublishMessage>(slaveLooper, testLooper);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);

    slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue, inControlQueue, slaveMetricsOut);
    threadsPool = Executors.newSingleThreadExecutor();

    threadsPool.execute(slave);
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();

    tupleReceived = 0;
    customGroupingInfoInPrepare = null;

    testLooper.exitLoop();
    slaveLooper.exitLoop();
    threadsPool.shutdownNow();

    physicalPlan = null;
    testLooper = null;
    slaveLooper = null;
    outStreamQueue = null;
    inStreamQueue = null;

    slave = null;
    threadsPool = null;
  }

  /**
   * Test direct mapping grouping
   */
  @Test
  public void testDirectMappingGrouping() throws Exception {
    final DirectMappingGrouping myCustomGrouping = new DirectMappingGrouping();
    final String expectedCustomGroupingStringInPrepare = "test-spout+test-spout+default+[1]";

    physicalPlan = constructPhysicalPlan(myCustomGrouping);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(CUSTOM_GROUPING_INFO, customGroupingInfoInPrepare);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (outStreamQueue.size() != 0) {
            HeronTuples.HeronTupleSet set = outStreamQueue.poll();

            Assert.assertTrue(set.isInitialized());
            Assert.assertFalse(set.hasControl());
            Assert.assertTrue(set.hasData());

            HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();
            Assert.assertEquals(dataTupleSet.getStream().getId(), "default");
            Assert.assertEquals(dataTupleSet.getStream().getComponentName(), "test-spout");

            for (HeronTuples.HeronDataTuple dataTuple : dataTupleSet.getTuplesList()) {
              List<Integer> destTaskIds = dataTuple.getDestTaskIdsList();
              Assert.assertEquals(destTaskIds.size(), 1);
              Assert.assertEquals(destTaskIds.get(0), (Integer) 3);
              tupleReceived++;
            }
          }
          if (tupleReceived == 10) {
            testLooper.exitLoop();
            break;
          }
          SysUtils.sleep(Constants.RETRY_INTERVAL_MS);
        }
      }
    };

    testLooper.addTasksOnWakeup(task);
    testLooper.loop();
  }

  /**
   * Test direct mapping grouping
   */
  @Test
  public void testDirectMappingGrouping2() throws Exception {
    final DirectMappingGrouping myCustomGrouping = new DirectMappingGrouping();
    final String expectedCustomGroupingStringInPrepare = "test-spout+test-spout+default+[1]";

    physicalPlan = constructPhysicalPlan2(myCustomGrouping);

    PhysicalPlanHelper physicalPlanHelper = new PhysicalPlanHelper(physicalPlan, SPOUT_INSTANCE_ID);
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(physicalPlanHelper).
        build();

    inControlQueue.offer(instanceControlMsg);

    SingletonRegistry.INSTANCE.registerSingleton(CUSTOM_GROUPING_INFO, customGroupingInfoInPrepare);

    Runnable task = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < Constants.RETRY_TIMES; i++) {
          if (outStreamQueue.size() != 0) {
            HeronTuples.HeronTupleSet set = outStreamQueue.poll();

            Assert.assertTrue(set.isInitialized());
            Assert.assertFalse(set.hasControl());
            Assert.assertTrue(set.hasData());

            HeronTuples.HeronDataTupleSet dataTupleSet = set.getData();
            Assert.assertEquals(dataTupleSet.getStream().getId(), "default");
            Assert.assertEquals(dataTupleSet.getStream().getComponentName(), "test-spout");

            for (HeronTuples.HeronDataTuple dataTuple : dataTupleSet.getTuplesList()) {
              List<Integer> destTaskIds = dataTuple.getDestTaskIdsList();
              Assert.assertEquals(destTaskIds.size(), 1);
              //Assert.assertEquals(destTaskIds.get(0), (Integer) 3);
              tupleReceived++;
            }
          }
          if (tupleReceived == 10) {
            testLooper.exitLoop();
            break;
          }
          SysUtils.sleep(Constants.RETRY_INTERVAL_MS);
        }
      }
    };

    testLooper.addTasksOnWakeup(task);
    testLooper.loop();
  }

  private PhysicalPlans.PhysicalPlan constructPhysicalPlan(
      DirectMappingGrouping directMappingGrouping) {
    PhysicalPlans.PhysicalPlan.Builder pPlan = PhysicalPlans.PhysicalPlan.newBuilder();

    // Set topology protobuf
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 3);
    // Here we need case switch to corresponding grouping
    topologyBuilder.setBolt("test-bolt", new TestBolt(), 1)
        .customGrouping("test-spout", directMappingGrouping);

    Config conf = new Config();
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setEnableAcking(false);

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    pPlan.setTopology(fTopology);

    // Set instances
    // Construct the spoutInstance
    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo.setComponentName("test-spout");
    spoutInstanceInfo.setTaskId(0);
    spoutInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance = PhysicalPlans.Instance.newBuilder();
    spoutInstance.setInstanceId("spout-id");
    spoutInstance.setStmgrId("stream-manager-id");
    spoutInstance.setInfo(spoutInstanceInfo);

    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo1 = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo1.setComponentName("test-spout");
    spoutInstanceInfo1.setTaskId(1);
    spoutInstanceInfo1.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance1 = PhysicalPlans.Instance.newBuilder();
    spoutInstance1.setInstanceId("spout-id");
    spoutInstance1.setStmgrId("stream-manager-id");
    spoutInstance1.setInfo(spoutInstanceInfo1);

    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo2 = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo2.setComponentName("test-spout");
    spoutInstanceInfo2.setTaskId(2);
    spoutInstanceInfo2.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance2 = PhysicalPlans.Instance.newBuilder();
    spoutInstance2.setInstanceId("spout-id");
    spoutInstance2.setStmgrId("stream-manager-id");
    spoutInstance2.setInfo(spoutInstanceInfo2);

    // Construct the boltInstanceInfo
    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo.setComponentName("test-bolt");
    boltInstanceInfo.setTaskId(3);
    boltInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance = PhysicalPlans.Instance.newBuilder();
    boltInstance.setInstanceId("bolt-id");
    boltInstance.setStmgrId("stream-manager-id");
    boltInstance.setInfo(boltInstanceInfo);

    pPlan.addInstances(spoutInstance);
    pPlan.addInstances(spoutInstance1);
    pPlan.addInstances(spoutInstance2);

    pPlan.addInstances(boltInstance);

    // Set stream mgr
    PhysicalPlans.StMgr.Builder stmgr = PhysicalPlans.StMgr.newBuilder();
    stmgr.setId("stream-manager-id");
    stmgr.setHostName("127.0.0.1");
    stmgr.setDataPort(8888);
    stmgr.setLocalEndpoint("endpoint");
    pPlan.addStmgrs(stmgr);

    return pPlan.build();
  }

  private PhysicalPlans.PhysicalPlan constructPhysicalPlan2(
      DirectMappingGrouping directMappingGrouping) {
    PhysicalPlans.PhysicalPlan.Builder pPlan = PhysicalPlans.PhysicalPlan.newBuilder();

    // Set topology protobuf
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    topologyBuilder.setSpout("test-spout", new TestSpout(), 2);
    // Here we need case switch to corresponding grouping
    topologyBuilder.setBolt("test-bolt", new TestBolt(), 6)
        .customGrouping("test-spout", directMappingGrouping);

    Config conf = new Config();
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setEnableAcking(false);

    TopologyAPI.Topology fTopology =
        topologyBuilder.createTopology().
            setName("topology-name").
            setConfig(conf).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    pPlan.setTopology(fTopology);

    // Set instances
    // Construct the spoutInstance
    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo.setComponentName("test-spout");
    spoutInstanceInfo.setTaskId(0);
    spoutInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance = PhysicalPlans.Instance.newBuilder();
    spoutInstance.setInstanceId("spout-id");
    spoutInstance.setStmgrId("stream-manager-id");
    spoutInstance.setInfo(spoutInstanceInfo);

    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo1 = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo1.setComponentName("test-spout");
    spoutInstanceInfo1.setTaskId(1);
    spoutInstanceInfo1.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance1 = PhysicalPlans.Instance.newBuilder();
    spoutInstance1.setInstanceId("spout-id");
    spoutInstance1.setStmgrId("stream-manager-id");
    spoutInstance1.setInfo(spoutInstanceInfo1);


    // Construct the boltInstanceInfo
    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo.setComponentName("test-bolt");
    boltInstanceInfo.setTaskId(2);
    boltInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance = PhysicalPlans.Instance.newBuilder();
    boltInstance.setInstanceId("bolt-id");
    boltInstance.setStmgrId("stream-manager-id");
    boltInstance.setInfo(boltInstanceInfo);

    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo1 = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo1.setComponentName("test-bolt");
    boltInstanceInfo1.setTaskId(3);
    boltInstanceInfo1.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance1 = PhysicalPlans.Instance.newBuilder();
    boltInstance1.setInstanceId("bolt-id");
    boltInstance1.setStmgrId("stream-manager-id");
    boltInstance1.setInfo(boltInstanceInfo1);

    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo2 = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo2.setComponentName("test-bolt");
    boltInstanceInfo2.setTaskId(4);
    boltInstanceInfo2.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance2 = PhysicalPlans.Instance.newBuilder();
    boltInstance2.setInstanceId("bolt-id");
    boltInstance2.setStmgrId("stream-manager-id");
    boltInstance2.setInfo(boltInstanceInfo2);

    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo3 = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo3.setComponentName("test-bolt");
    boltInstanceInfo3.setTaskId(5);
    boltInstanceInfo3.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance3 = PhysicalPlans.Instance.newBuilder();
    boltInstance3.setInstanceId("bolt-id");
    boltInstance3.setStmgrId("stream-manager-id");
    boltInstance3.setInfo(boltInstanceInfo3);

    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo4 = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo4.setComponentName("test-bolt");
    boltInstanceInfo4.setTaskId(6);
    boltInstanceInfo4.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance4 = PhysicalPlans.Instance.newBuilder();
    boltInstance4.setInstanceId("bolt-id");
    boltInstance4.setStmgrId("stream-manager-id");
    boltInstance4.setInfo(boltInstanceInfo4);

    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo5 = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo5.setComponentName("test-bolt");
    boltInstanceInfo5.setTaskId(7);
    boltInstanceInfo5.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance5 = PhysicalPlans.Instance.newBuilder();
    boltInstance5.setInstanceId("bolt-id");
    boltInstance5.setStmgrId("stream-manager-id");
    boltInstance5.setInfo(boltInstanceInfo5);

    pPlan.addInstances(spoutInstance);
    pPlan.addInstances(spoutInstance1);

    pPlan.addInstances(boltInstance);
    pPlan.addInstances(boltInstance1);
    pPlan.addInstances(boltInstance2);
    pPlan.addInstances(boltInstance3);
    pPlan.addInstances(boltInstance4);
    pPlan.addInstances(boltInstance5);

    // Set stream mgr
    PhysicalPlans.StMgr.Builder stmgr = PhysicalPlans.StMgr.newBuilder();
    stmgr.setId("stream-manager-id");
    stmgr.setHostName("127.0.0.1");
    stmgr.setDataPort(8888);
    stmgr.setLocalEndpoint("endpoint");
    pPlan.addStmgrs(stmgr);

    return pPlan.build();
  }
}
