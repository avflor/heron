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

import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.SymptomDetectorService;
import com.twitter.heron.healthmgr.symptomdetector.BackPressureDetector;
import com.twitter.heron.healthmgr.symptomdetector.ReportingDetector;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDiagnoser;
import com.twitter.heron.spi.healthmgr.InstanceSymptom;


public class LowInputThroughputDiagnoser implements IDiagnoser<ComponentSymptom> {

  private static final String EMIT_COUNT_METRIC = "__emit-count/default";


  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector emitCountDetector = new ReportingDetector(EMIT_COUNT_METRIC);
  private TopologyAPI.Topology topology;
  private double maxThroughput = 0;
  private SymptomDetectorService symptomDetectorService;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);

    backpressureDetector.initialize(conf, runtime);
    emitCountDetector.initialize(conf, runtime);

    symptomDetectorService = (SymptomDetectorService) Runtime
        .getSymptomDetectorService(runtime);
  }

  public void setSpoutThroughput(double throughput) {
    this.maxThroughput = throughput;
  }

  @Override
  public Diagnosis<ComponentSymptom> diagnose(TopologyAPI.Topology topology) {
    double totalEmitCount = 0;
    Set<ComponentSymptom> backPressureSymptom =
        symptomDetectorService.run(backpressureDetector, topology);

    Set<ComponentSymptom> emitCountSymptom =
        symptomDetectorService.run(emitCountDetector, topology);

    if (backPressureSymptom.size() == 0) {
      System.out.println("No backpressure");

      List<TopologyAPI.Spout> spouts = topology.getSpoutsList();
      for (ComponentSymptom current : emitCountSymptom) {
        int position = contains(spouts, current.getComponentName());
        if (position != -1) {
          Diagnosis<ComponentSymptom> spoutDiagnosis = new Diagnosis<ComponentSymptom>();
          for (InstanceSymptom instanceData : current.getInstances()) {
            double emitCount =
                Double.valueOf(instanceData.getDataPoint(EMIT_COUNT_METRIC));
            totalEmitCount += emitCount;
          }
          System.out.println(totalEmitCount + " " + maxThroughput);
          if (totalEmitCount < maxThroughput) {
            spoutDiagnosis.addToDiagnosis(current);
            return spoutDiagnosis;
          }

        }
      }
    }
    return null;
  }

  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentSymptom> oldDiagnosis,
                                  Diagnosis<ComponentSymptom> newDiagnosis) {

    /*Set<ComponentSymptom> oldSummary = oldDiagnosis.getSummary();
    Set<ComponentSymptom> newSummary = newDiagnosis.getSummary();
    ComponentSymptom oldComponent = oldSummary.iterator().next();
    ComponentSymptom newComponent = newSummary.iterator().next();
    System.out.println("old " + oldComponent.toString());
    System.out.println("new " + newComponent.toString());
    if (!oldComponent.getComponentName().equals(newComponent.getComponentName())
        || !HealthManagerUtils.containsInstanceIds(oldComponent, newComponent)) {
      System.out.println("first");
      return false;
    } else {
      if (!HealthManagerUtils.similarBackPressure(oldComponent, newComponent)) {
        System.out.println("second");
        return false;
      }
      if (!HealthManagerUtils.similarSumMetric(oldComponent, newComponent,
          EXECUTION_COUNT_METRIC, 2)) {
        System.out.println("third");
        return false;
      }
    }
    return true;*/
    return false;
  }

  @Override
  public void close() {
    backpressureDetector.close();
    emitCountDetector.close();
  }

  private int contains(List<TopologyAPI.Spout> spouts, String name) {
    for (int i = 0; i < spouts.size(); i++) {
      if (spouts.get(i).getComp().getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

}
