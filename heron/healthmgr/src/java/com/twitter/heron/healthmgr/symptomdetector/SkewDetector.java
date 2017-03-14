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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.outlierdetection.SimpleMADOutlierDetector;
import com.twitter.heron.healthmgr.utils.HealthManagerUtils;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.ThresholdBasedDetector;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;

/**
 * Detects the instances that have data skew.
 */
public class SkewDetector extends ThresholdBasedDetector<ComponentSymptom> {

  private SinkVisitor visitor;
  private String metric;

  public SkewDetector(String metric, double threshold) {
    super(threshold);
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

    Set<ComponentSymptom> symptoms = new HashSet<ComponentSymptom>();

    for (int i = 0; i < boltNames.length; i++) {
      String component = bolts.get(i).getComp().getName();
      Iterable<MetricsInfo> metricsResults = this.visitor.getNextMetric(metric,
          component);

      //detect outliers
      Double[] dataPoints = HealthManagerUtils.getDoubleDataPoints(metricsResults);
      SimpleMADOutlierDetector outlierDetector = new SimpleMADOutlierDetector(this.getThreshold());
      ArrayList<Integer> outliers = outlierDetector.detectOutliers(dataPoints);

      if (outliers.size() != 0) {
        ComponentSymptom currentSymptom;
        currentSymptom = new ComponentSymptom(component);
        for (int j = 0; j < outliers.size(); j++) {
          int current = 0;
          for (MetricsInfo metricsInfo : metricsResults) {
            //System.out.println(current + " " + j + outliers.get(j));
            if (current == outliers.get(j)) {
              HealthManagerUtils.updateComponentSymptom(currentSymptom, metric,
                  metricsInfo);
            }
            current++;
          }
        }
        symptoms.add(currentSymptom);
      }

    }
    return symptoms;
  }


  @Override
  public void close() {
  }

}


