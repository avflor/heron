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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.utils.HealthManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.ISymptomDetector;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;


public class ReportingDetector implements ISymptomDetector<ComponentSymptom> {

  private SinkVisitor visitor;
  private String metric;

  public ReportingDetector(String metric) {
    this.metric = metric;
  }

  @Override
  public void initialize(Config config, Config runtime) {
    this.visitor = Runtime.metricsReader(runtime);
  }

  @Override
  public Set<ComponentSymptom> detect(TopologyAPI.Topology topology) {
    List<TopologyAPI.Bolt> bolts = topology.getBoltsList();
    String[] boltNames = new String[bolts.size()];

    List<TopologyAPI.Spout> spouts = topology.getSpoutsList();
    String[] spoutNames = new String[spouts.size()];

    Set<ComponentSymptom> symptoms = new HashSet<ComponentSymptom>();

    for (int i = 0; i < boltNames.length; i++) {
      String component = bolts.get(i).getComp().getName();
      updateSymptoms(symptoms, component);
    }

    for (int i = 0; i < spoutNames.length; i++) {
      String component = spouts.get(i).getComp().getName();
      updateSymptoms(symptoms, component);
    }
    //System.out.println(symptoms.toString());
    return symptoms;
  }


  private void updateSymptoms(Set<ComponentSymptom> symptoms, String component) {
    Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric(metric, component);

    ComponentSymptom currentSymptom = new ComponentSymptom(component);
    for (MetricsInfo metricsInfo : metricsResults) {
      HealthManagerUtils.updateComponentSymptom(currentSymptom, metric, metricsInfo);
    }
    symptoms.add(currentSymptom);
  }

  @Override
  public void close() {
  }
}


