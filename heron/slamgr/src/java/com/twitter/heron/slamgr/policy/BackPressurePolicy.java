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


package com.twitter.heron.slamgr.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.slamgr.TopologyGraph;
import com.twitter.heron.slamgr.clustering.DiscreteValueClustering;
import com.twitter.heron.slamgr.detector.BackPressureDetector;
import com.twitter.heron.slamgr.detector.ReportingDetector;
import com.twitter.heron.slamgr.outlierdetection.SimpleMADOutlierDetector;
import com.twitter.heron.slamgr.resolver.ScaleUpResolver;
import com.twitter.heron.slamgr.utils.SLAManagerUtils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.Bottleneck;
import com.twitter.heron.spi.slamgr.ComponentBottleneck;
import com.twitter.heron.spi.slamgr.Diagnosis;
import com.twitter.heron.spi.slamgr.InstanceBottleneck;
import com.twitter.heron.spi.slamgr.SLAPolicy;
import com.twitter.heron.spi.slamgr.utils.BottleneckUtils;

import static com.twitter.heron.spi.slamgr.utils.BottleneckUtils.getComponentBottleneck;



public class BackPressurePolicy implements SLAPolicy {

  enum Problem {
    SLOW_INSTANCE, DATA_SKEW, LIMITED_PARALLELISM
  }
  private final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private final String EXECUTION_COUNT_METRIC = "__execute-count/default";
  private final String EMIT_COUNT_METRIC = "__emit-count/default";

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);
  private ScaleUpResolver scaleUpResolver = new ScaleUpResolver();
  private ReportingDetector emitCountDetector = new ReportingDetector(EMIT_COUNT_METRIC);

  private TopologyAPI.Topology topology;
  private TopologyGraph topologyGraph;

  private ArrayList<String> topologySort = null;


  @Override
  public void initialize(Config conf, Config runtime, TopologyAPI.Topology t,
                         SinkVisitor visitor) {
    this.topology = t;
    this.topologyGraph = new TopologyGraph();
    backpressureDetector.initialize(conf, runtime, visitor);
    executeCountDetector.initialize(conf, runtime, visitor);
    emitCountDetector.initialize(conf, runtime, visitor);
    scaleUpResolver.initialize(conf, runtime);
  }

  @Override
  public void execute() {
    Diagnosis<ComponentBottleneck> backPressuredDiagnosis = backpressureDetector.detect(topology);
    Diagnosis<ComponentBottleneck> executeCountDiagnosis = executeCountDetector.detect(topology);
    boolean found = false;

    if (backPressuredDiagnosis != null && executeCountDiagnosis != null) {

      Set<ComponentBottleneck> backPressureSummary = backPressuredDiagnosis.getSummary();
      Set<ComponentBottleneck> executeCountSummary = executeCountDiagnosis.getSummary();

      if (backPressureSummary.size() != 0 && executeCountSummary.size() != 0) {
        BottleneckUtils.merge(backPressureSummary, executeCountSummary);

        if (topologySort == null) {
          topologySort = getTopologySort(topology);
        }
        for (int i = 0; i < topologySort.size() && !found; i++) {
          String name = topologySort.get(i);
          ComponentBottleneck current = getComponentBottleneck(backPressureSummary, name);
          if (current != null) {
            Problem problem = identifyProblem(current);
            if(problem == Problem.LIMITED_PARALLELISM){
              double scaleFactor = computeScaleUpFactor(current, executeCountSummary);
              int newParallelism = (int)Math.ceil(current.getInstances().size() * scaleFactor);
              System.out.println("scale factor" + scaleFactor + " " + newParallelism);

              if (false) {
                Diagnosis<ComponentBottleneck> currentDiagnosis = new Diagnosis<>();
                currentDiagnosis.addToDiagnosis(current);
                 newParallelism = (int)Math.ceil(current.getInstances().size() * scaleFactor);
                scaleUpResolver.setParallelism(newParallelism);
                scaleUpResolver.resolve(currentDiagnosis, topology);
                found = true;
              }
            }
          }
        }
      }
    }
  }


  private double computeScaleUpFactor(ComponentBottleneck current, Set<ComponentBottleneck>
      executeCountSummary) {
    Diagnosis<ComponentBottleneck> emitCountDiagnosis =
        emitCountDetector.detect(topology);
    Set<ComponentBottleneck> emitCountSummary = emitCountDiagnosis.getSummary();
    Set<String> parentComponents = topologyGraph.getParents(current.getComponentName());
    double emitSum = 0;
    for(String name : parentComponents){
      emitSum += BottleneckUtils.computeSum(emitCountSummary, name, EMIT_COUNT_METRIC);
    }
    double executeCountSum = BottleneckUtils.computeSum(executeCountSummary,
        current.getComponentName(), EXECUTION_COUNT_METRIC);

    System.out.println("LLL " + emitSum + " " + executeCountSum + " " +emitSum/executeCountSum);
    return emitSum/executeCountSum;
  }


  private Problem identifyProblem(ComponentBottleneck current) {
    Double[] backPressureDataPoints = current.getDataPoints(BACKPRESSURE_METRIC);
    DiscreteValueClustering clustering = new DiscreteValueClustering();
    HashMap<String, ArrayList<Integer>> backPressureClusters =
        clustering.createBinaryClusters(backPressureDataPoints, 0.0);

    /*Double[] executionCountDataPoints = current.getDataPoints(EXECUTION_COUNT_METRIC);
    SimpleMADOutlierDetector executeCountOutlierDetector = new SimpleMADOutlierDetector(1.0);
    ArrayList<Integer> executeCountOutliers =
        executeCountOutlierDetector.detectOutliers(executionCountDataPoints);*/

    if (backPressureClusters.get("1.0").size() <
        (10 * backPressureClusters.get("0.0").size()) / 100) {
      switch (compareExecuteCounts(current)) {
        case 0:
          return Problem.LIMITED_PARALLELISM;
        case -1:
          return Problem.SLOW_INSTANCE;
        case 1:
          return Problem.DATA_SKEW;
      }
    } else {
      return Problem.LIMITED_PARALLELISM;
    }
    return null;
  }

  private int compareExecuteCounts(ComponentBottleneck bottleneck) {

    double backPressureExecuteCounts = 0;
    double nonBackPressureExecuteCounts = 0;
    int noBackPressureInstances = 0;
    for (int j = 0; j < bottleneck.getInstances().size(); j++) {
      InstanceBottleneck currentInstance = bottleneck.getInstances().get(j);
      if (!currentInstance.getInstanceData().getMetricValue(BACKPRESSURE_METRIC).equals("0.0")) {
        backPressureExecuteCounts += Double.parseDouble(
            currentInstance.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC));
        noBackPressureInstances++;
      } else {
        nonBackPressureExecuteCounts += Double.parseDouble(
            currentInstance.getInstanceData().getMetricValue(EXECUTION_COUNT_METRIC));
      }
    }
    int noNonBackPressureInstances = bottleneck.getInstances().size() - noBackPressureInstances;
    if (backPressureExecuteCounts / noBackPressureInstances > 2 * (
        nonBackPressureExecuteCounts / noNonBackPressureInstances)) {
      return 1;
    } else {
      if (backPressureExecuteCounts / noBackPressureInstances < 0.5 * (
          nonBackPressureExecuteCounts / noNonBackPressureInstances)) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  @Override
  public void close() {
    backpressureDetector.close();
    executeCountDetector.close();
    scaleUpResolver.close();
  }

  private ArrayList<String> getTopologySort(TopologyAPI.Topology topology) {
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String boltName = bolt.getComp().getName();

      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        this.topologyGraph.addEdge(parent, boltName);
      }
    }
    TopologyGraph tmp = new TopologyGraph(this.topologyGraph);

    return tmp.topologicalSort();
  }
}
