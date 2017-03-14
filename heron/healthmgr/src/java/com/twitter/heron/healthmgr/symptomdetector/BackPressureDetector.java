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
package com.twitter.heron.healthmgr.symptomdetector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.utils.HealthManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.ISymptomDetector;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;

public class BackPressureDetector implements ISymptomDetector<ComponentSymptom> {

  private static final Logger LOG = Logger.getLogger(BackPressureDetector.class.getName());
  private static final String BACKPRESSURE_METRIC = "__time_spent_back_pressure_by_compid";
  private SinkVisitor visitor;
  private Config runtime;


  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.visitor = Runtime.metricsReader(runtime);
  }

  @Override
  public Set<ComponentSymptom> detect(TopologyAPI.Topology topology)
      throws RuntimeException {

    LOG.info("Executing: " + this.getClass().getName());
    PackingPlan packingPlan = Runtime.packingPlan(runtime);
    HashMap<String, ComponentSymptom> results = HealthManagerUtils.retrieveMetricValues(
        BACKPRESSURE_METRIC, "", "__stmgr__", this.visitor, packingPlan);

    Set<ComponentSymptom> symptoms = new HashSet<ComponentSymptom>();
    for (ComponentSymptom symptom : results.values()) {
      if (symptom.containsNonZero(BACKPRESSURE_METRIC)) {
        System.out.println("symptom name " + symptom.getComponentName().toString());
        symptoms.add(symptom);
      }
    }
    return symptoms;
  }

  @Override
  public void close() {
  }
}


