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
package com.twitter.heron.spi.healthmgr;


import java.util.ArrayList;
import java.util.Set;

import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.packing.InstanceId;
import com.twitter.heron.spi.packing.PackingPlan.InstancePlan;

public class ComponentSymptom extends Symptom {
  private String componentName;
  private ArrayList<InstanceSymptom> instances;

  public ComponentSymptom(String componentName) {
    this.componentName = componentName;
    this.instances = new ArrayList<>();
  }

  public void add(int containerId, InstancePlan instance, Set<MetricsInfo> metrics) {
    instances.add(new InstanceSymptom(containerId, instance, metrics));
  }


  public void merge(ComponentSymptom symptom) {
    for (InstanceSymptom InstanceSymptom : instances) {
      for (InstanceSymptom newInstanceSymptom : symptom.getInstances()) {
        InstanceInfo currentData = InstanceSymptom.getInstanceData();
        InstanceInfo newData = newInstanceSymptom.getInstanceData();
        if (currentData.getInstanceId() == newData.getInstanceId()
            && currentData.getContainerId() == currentData.getContainerId()) {
          currentData.updateMetrics(newData.getMetrics());
        }
      }
    }
  }

  public String toString() {
    return componentName + " " + instances.toString();
  }

  public String getComponentName() {
    return componentName;
  }

  public ArrayList<InstanceSymptom> getInstances() {
    return instances;
  }

  public boolean contains(String metric, String value) {
    for (InstanceSymptom InstanceSymptom : instances) {
      if (InstanceSymptom.contains(metric, value)) {
        return true;
      }
    }
    return false;
  }

  public boolean containsBelow(String metric, String value){
    int count = 0;
    for (InstanceSymptom InstanceSymptom : instances) {
      if (InstanceSymptom.containsBelow(metric, value)) {
        count++;
      }
    }
    if (count == instances.size()) {
      return true;
    }
    return false;
  }

  public boolean containsNonZero(String metric) {
    int count = 0;
    for (InstanceSymptom InstanceSymptom : instances) {
      if (InstanceSymptom.contains(metric, "0")) {
        count++;
      }
    }
    if (count == instances.size()) {
      return false;
    }
    return true;
  }

  public Double[] getDataPoints(String metric) {
    Double[] dataPoints = new Double[instances.size()];
    for (int i = 0; i < instances.size(); i++) {
      dataPoints[i] = Double.parseDouble(instances.get(i).getDataPoint(metric));
    }
    return dataPoints;
  }
}

