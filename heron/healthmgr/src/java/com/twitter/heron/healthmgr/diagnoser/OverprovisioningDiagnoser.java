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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.services.SymptomDetectorService;
import com.twitter.heron.healthmgr.symptomdetector.BackPressureDetector;
import com.twitter.heron.healthmgr.utils.HealthManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDiagnoser;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.packing.PackingPlan;

public class OverprovisioningDiagnoser implements IDiagnoser<ComponentSymptom> {

  private static final Logger LOG = Logger.getLogger(OverprovisioningDiagnoser.class.getName());
  private static final String AVG_PENDING_PACKETS = "__connection_buffer_by_intanceid";
  private SinkVisitor visitor;
  private Config runtime;
  private int packetThreshold = 0;
  private BackPressureDetector backpressureDetector = new BackPressureDetector();
  private SymptomDetectorService detectorService;

  @Override
  public void initialize(Config inputConfig, Config inputRuntime) {
    this.runtime = inputRuntime;
    this.visitor = Runtime.metricsReader(runtime);
    this.backpressureDetector.initialize(inputConfig, runtime);
    detectorService = (SymptomDetectorService) Runtime
        .getSymptomDetectorService(runtime);
  }

  public void setPacketThreshold(int noPackets){
    this.packetThreshold = noPackets;
  }

  @Override
  public Diagnosis<ComponentSymptom> diagnose(TopologyAPI.Topology topology)
      throws RuntimeException {

    Set<ComponentSymptom> backPressuredSymptom =
        detectorService.run(backpressureDetector, topology);
    if(backPressuredSymptom.size() == 0) {
      LOG.info("Executing: " + this.getClass().getName());
      PackingPlan packingPlan = Runtime.packingPlan(runtime);
      HashMap<String, ComponentSymptom> results = HealthManagerUtils.retrieveMetricValues(
          AVG_PENDING_PACKETS, "packets", "__stmgr__", this.visitor, packingPlan);

      List<TopologyAPI.Spout> spouts = topology.getSpoutsList();

      Set<ComponentSymptom> symptoms = new HashSet<ComponentSymptom>();
      for (ComponentSymptom symptom : results.values()) {
        System.out.println(symptom.toString()) ;
        int position = contains(spouts, symptom.getComponentName());
        if (position == -1 &&
            symptom.containsBelow(AVG_PENDING_PACKETS, String.valueOf(packetThreshold))) {
          //System.out.println("symptom name " + symptom.getComponentName().toString());
          symptoms.add(symptom);
        }
      }
      return new Diagnosis<ComponentSymptom>(symptoms);
    }
    return null;
  }


  private int contains(List<TopologyAPI.Spout> spouts, String name) {
    for (int i = 0; i < spouts.size(); i++) {
      if (spouts.get(i).getComp().getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public boolean similarDiagnosis(Diagnosis<ComponentSymptom> firstDiagnosis,
                                  Diagnosis<ComponentSymptom> secondDiagnosis){
    return false;
  }


  @Override
  public void close() {
    this.backpressureDetector.close();
  }
}


