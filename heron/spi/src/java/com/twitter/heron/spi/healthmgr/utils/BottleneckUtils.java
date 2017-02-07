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

import com.twitter.heron.spi.healthmgr.ComponentBottleneck;

public final class BottleneckUtils {

  private BottleneckUtils() {
  }

  public static ComponentBottleneck getComponentBottleneck(Set<ComponentBottleneck> summary,
                                                           String component) {
    for (ComponentBottleneck bottleneck : summary) {
      if (bottleneck.getComponentName().equals(component)) {
        return bottleneck;
      }
    }
    return null;
  }

  public static void merge(Set<ComponentBottleneck> firstSummary,
                           Set<ComponentBottleneck> secondSummary) {
    for (ComponentBottleneck firstBottleneck : firstSummary) {
      for (ComponentBottleneck secondBottleneck : secondSummary) {
        if (firstBottleneck.getComponentName().equals(secondBottleneck.getComponentName())) {
          firstBottleneck.merge(secondBottleneck);
        }
      }
    }
  }

  public static int computeSum(Set<ComponentBottleneck> summary,
                               String componentMame, String metricName) {
    int sum = 0;
    ComponentBottleneck tmp = getComponentBottleneck(summary, componentMame);
    Double[] data = tmp.getDataPoints(metricName);
    for (int i = 0; i < data.length; i++) {
      sum += data[i];
    }
    return sum;
  }
}
