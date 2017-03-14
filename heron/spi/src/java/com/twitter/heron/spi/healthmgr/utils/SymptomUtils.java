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

package com.twitter.heron.spi.healthmgr.utils;


import java.util.Set;

import com.twitter.heron.spi.healthmgr.ComponentSymptom;

public final class SymptomUtils {

  private SymptomUtils() {
  }

  public static ComponentSymptom getComponentSymptom(Set<ComponentSymptom> summary,
                                                           String component) {
    for (ComponentSymptom symptom : summary) {
      if (symptom.getComponentName().equals(component)) {
        return symptom;
      }
    }
    return null;
  }

  public static void merge(Set<ComponentSymptom> firstSummary,
                           Set<ComponentSymptom> secondSummary) {
    for (ComponentSymptom firstSymptom : firstSummary) {
      for (ComponentSymptom secondSymptom : secondSummary) {
        if (firstSymptom.getComponentName().equals(secondSymptom.getComponentName())) {
          firstSymptom.merge(secondSymptom);
        }
      }
    }
  }

  public static int computeSum(ComponentSymptom symptom, String metricName) {
    int sum = 0;
    Double[] data = symptom.getDataPoints(metricName);
    for (int i = 0; i < data.length; i++) {
      sum += data[i];
    }
    return sum;
  }

}
