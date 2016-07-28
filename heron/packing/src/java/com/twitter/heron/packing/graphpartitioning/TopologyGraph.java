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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class TopologyGraph {

  private int numVertices = 0;
  private int numEdges = 0;
  private int noVWeights = 0;
  private int maxLevel = 0;
  private HashMap<Integer, Vertex> vertices = new HashMap<Integer, Vertex>();
  private HashMap<Integer, ArrayList<Edge>> connections = new HashMap<Integer, ArrayList<Edge>>();
  private HashMap<Integer, ArrayList<Integer>> levelVertices
      = new HashMap<Integer, ArrayList<Integer>>();
  private String name = null;

  public TopologyGraph(String name, int noVWeights) {
    this.name = name;
    this.noVWeights = noVWeights;
  }

  public int getNumVertices() {
    return numVertices;
  }

  public void setNumVertices(int v) {
    numVertices = v;
  }

  public int getNumEdges() {
    return numEdges;
  }

  public void setNumEdges(int e) {
    numEdges = e;
  }

  public int getNoVWeights() {
    return noVWeights;
  }

  public void setNoVWeights(int noVWeights) {
    this.noVWeights = noVWeights;
  }

  public HashMap<Integer, Vertex> getVertices() {
    return vertices;
  }

  public void setVertices(HashMap<Integer, Vertex> vertices) {
    this.vertices = vertices;
  }

  public HashMap<Integer, ArrayList<Edge>> getConnections() {
    return connections;
  }

  public void setConnections(HashMap<Integer, ArrayList<Edge>> connections) {
    this.connections = connections;
  }

  public HashMap<Integer, ArrayList<Integer>> getLevelVertices() {
    return levelVertices;
  }

  public void setLevelVertices(HashMap<Integer, ArrayList<Integer>> levelVertices) {
    this.levelVertices = levelVertices;
  }

  public int getMaxLevel() {
    return maxLevel;
  }

  public void setMaxLevel(int maxLevel) {
    this.maxLevel = maxLevel;
  }

  public ArrayList<Integer> getVerticesOfLevel(int level) {
    return levelVertices.get(level);
  }

  public Vertex getVertex(int vertexId) {
    return vertices.get(vertexId);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void addVertex(Vertex v) {
    numVertices++;
    vertices.put(v.getVertexId(), v);
    if (!levelVertices.containsKey(v.getLevel())) {
      levelVertices.put(v.getLevel(), new ArrayList<Integer>());
    }
    levelVertices.get(v.getLevel()).add(v.getVertexId());
    if (!connections.containsKey(v.getVertexId())) {
      connections.put(v.getVertexId(), new ArrayList<Edge>());
    }
    if (v.getLevel() > maxLevel) {
      maxLevel = v.getLevel();
    }
  }

  public boolean hasVertex(Vertex v) {
    if (connections.containsKey(v)) {
      return true;
    }
    return false;
  }

  public void addEdge(Edge e) {
    boolean added = false;
    if (!connections.containsKey(e.getVertex1())) {
      connections.put(e.getVertex1(), new ArrayList<Edge>());
    }
    if (!connections.containsKey(e.getVertex2())) {
      connections.put(e.getVertex2(), new ArrayList<Edge>());
    }
    if (!connections.get(e.getVertex1()).contains(e)
        && !connections.get(e.getVertex1()).contains(e.getReverse())) {
      connections.get(e.getVertex1()).add(e);
      added = true;
    }
    if (!connections.get(e.getVertex2()).contains(e)
        && !connections.get(e.getVertex2()).contains(e.getReverse())) {
      connections.get(e.getVertex2()).add(e);
      added = true;
    }
    if (added) {
      numEdges++;
    }
  }

  public ArrayList<Integer> getNeighbours(Integer v) {
    ArrayList<Integer> neighbours = new ArrayList<Integer>();
    ArrayList<Edge> neighbourEdges = connections.get(v);
    for (int i = 0; i < neighbourEdges.size(); i++) {
      Edge next = neighbourEdges.get(i);
      int v1 = next.getVertex1();
      if (v != v1) {
        neighbours.add(v1);
      } else {
        int v2 = next.getVertex2();
        neighbours.add(v2);
      }
    }
    return neighbours;
  }

  public int getNeighbour(Integer v, Edge e) {
    if (v != e.getVertex1()) {
      return e.getVertex1();
    } else {
      return e.getVertex2();
    }
  }

  public void createMetisfile() {
    try {
      FileWriter outFile = new FileWriter(name);
      PrintWriter out = new PrintWriter(outFile);
      //first line
      out.println(numVertices + " " + numEdges + " " + "011" + " " + noVWeights);
      for (int id = 1; id <= numVertices; id++) {
        Vertex v = vertices.get(id);
        String weights = "";
        weights = weights + Math.round(v.getVertexWeights().getCpu()) + " "
            + v.getVertexWeights().getRam() / 1024 + " "
            + v.getVertexWeights().getDisk() / 1024;
        String adjacentVertices = "";
        ArrayList<Edge> neighbours = connections.get(id);
        //System.out.println("N " + neighbours.size());
        for (int j = 0; j < neighbours.size(); j++) {
          adjacentVertices = adjacentVertices + neighbours.get(j).getOtherVertex(id) + " "
              + neighbours.get(j).getWeight();
          if (j < neighbours.size() - 1) {
            adjacentVertices = adjacentVertices + " ";
          }
        }
        String line = weights + " " + adjacentVertices;
        out.println(line);
      }
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
