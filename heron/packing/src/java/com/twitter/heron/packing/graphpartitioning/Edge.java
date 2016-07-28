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
package com.twitter.heron.packing.graphpartitioning;

public class Edge {

  private int vertex1;
  private int vertex2;
  private int weight;

  public Edge() {
  }

  public Edge(int vertex1, int vertex2, int weight) {
    this.vertex1 = vertex1;
    this.vertex2 = vertex2;
    this.weight = weight;
  }

  public int getVertex1() {
    return vertex1;
  }

  public void setVertex1(int vertex1) {
    this.vertex1 = vertex1;
  }

  public int getVertex2() {
    return vertex2;
  }

  public void setVertex2(int vertex2) {
    this.vertex2 = vertex2;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  public Integer getOtherVertex(int v) {
    if (v == vertex1) {
      return vertex2;
    }
    return vertex1;
  }

  public Edge getReverse() {
    Edge e = new Edge(this.vertex2, this.vertex1, this.weight);
    return e;
  }

  @Override
  public boolean equals(Object e) {
    if (e == this) {
      return true;
    }
    if (e == null || e.getClass() != this.getClass()) {
      return false;
    }
    if (((Edge) e).vertex1 == this.vertex1 && ((Edge) e).vertex2 == this.vertex2
        && ((Edge) e).weight == this.weight) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return weight;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();

    result.append(vertex1 + " ");
    result.append(vertex2 + " ");
    result.append(weight + " ");

    return result.toString();
  }
}
