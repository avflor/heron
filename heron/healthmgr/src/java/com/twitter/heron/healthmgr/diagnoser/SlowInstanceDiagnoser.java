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
import com.twitter.heron.spi.healthmgr.utils.SymptomUtils;

public class SlowInstanceDiagnoser implements IDiagnoser<ComponentSymptom> {

  private static final String EXECUTION_COUNT_METRIC = "__execute-count/default";

  private Config runtime;

  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private ReportingDetector executeCountDetector = new ReportingDetector(EXECUTION_COUNT_METRIC);

  private SymptomDetectorService detectorService;

  @Override
  public void initialize(Config config, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.backpressureDetector.initialize(config, inputRuntime);
    this.executeCountDetector.initialize(config, inputRuntime);
    detectorService = (SymptomDetectorService) Runtime
        .getSymptomDetectorService(runtime);
  }

  @Override
  public Diagnosis<ComponentSymptom> diagnose(TopologyAPI.Topology topology) {
    Set<ComponentSymptom> backPressureSymptom =
        detectorService.run(backpressureDetector, topology);

    Set<ComponentSymptom> executeCountSymptom =
        detectorService.run(executeCountDetector, topology);


    if (backPressureSymptom.size() != 0 && executeCountSymptom.size() != 0) {
      SymptomUtils.merge(backPressureSymptom, executeCountSymptom);

      ComponentSymptom current = backPressureSymptom.iterator().next();
      if (existSlowInstances(current)) {
        Diagnosis<ComponentSymptom> currentDiagnosis = new Diagnosis<>();
        currentDiagnosis.addToDiagnosis(current);
        return currentDiagnosis;
      }
    }
    return null;
  }

  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentSymptom> firstDiagnosis,
                                  Diagnosis<ComponentSymptom> secondDiagnosis) {
    return true;
  }

  @Override
  public void close() {
    backpressureDetector.close();
    executeCountDetector.close();
  }

  private boolean existSlowInstances(ComponentSymptom current) {
    return DataSkewDiagnoser.compareExecuteCounts(current) == -1;
  }
}
