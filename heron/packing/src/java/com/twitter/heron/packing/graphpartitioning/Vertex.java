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

import com.twitter.heron.spi.packing.Resource;

public class Vertex {

  private int level;
  private int vertexId;
  private Resource vertexWeights;
  private String component;

  public Vertex(int id, int level, Resource vertexWeights, String component) {
    this.vertexId = id;
    this.vertexWeights = vertexWeights;
    this.level = level;
    this.component = component;
  }

  public int getVertexId() {
    return vertexId;
  }

  public Resource getVertexWeights() {
    return vertexWeights;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public String getComponent() {
    return component;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(vertexId);
    return result.toString();
  }
}
