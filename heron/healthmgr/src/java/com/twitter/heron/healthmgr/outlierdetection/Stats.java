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
package com.twitter.heron.healthmgr.outlierdetection;


public final class Stats {

  private Stats() {
  }

  public static double median(Double[] m) {
    int middle = m.length / 2;
    if (m.length % 2 == 1) {
      return m[middle];
    } else {
      return (m[middle - 1] + m[middle]) / 2.0;
    }
  }

  public static Double[] subtract(Double[] m, Double value) {
    Double[] result = new Double[m.length];
    for (int i = 0; i < m.length; i++) {
      result[i] = Math.abs(m[i] - value);
    }
    return result;
  }

  public static Double mad(Double[] m) {
    Double median = median(m);
    Double[] newData = subtract(m, median);
    return median(newData);
  }
}
