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


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

public class GraphPartitioningPacking implements IPacking {

  public static final long MIN_RAM_PER_INSTANCE = 192L * Constants.MB;
  public static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  public static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;

  private static final Logger LOG = Logger.getLogger(
      com.twitter.heron.packing.graphpartitioning.GraphPartitioningPacking.class.getName());
  protected TopologyAPI.Topology topology;

  protected long instanceRamDefault;
  protected double instanceCpuDefault;
  protected long instanceDiskDefault;
  protected long maxContainerRam;
  protected double maxContainerCpu;
  protected long maxContainerDisk;
  protected int numAdjustments;

  protected HashMap<String, ArrayList<Integer>> componentVertices;

  public static String getContainerId(int index) {
    return "" + index;
  }

  public static String getInstanceId(
      int containerIdx, String componentName, int instanceIdx, int componentIdx) {
    return String.format("%d:%s:%d:%d", containerIdx, componentName, instanceIdx, componentIdx);
  }

  public static String getComponentName(String instanceId) {
    return instanceId.split(":")[1];
  }

  @Override
  public void initialize(Config config, Config runtime) {
    this.topology = com.twitter.heron.spi.utils.Runtime.topology(runtime);
    this.instanceRamDefault = Context.instanceRam(config);
    this.instanceCpuDefault = Context.instanceCpu(config).doubleValue();
    this.instanceDiskDefault = Context.instanceDisk(config);
    this.componentVertices = new HashMap<String, ArrayList<Integer>>();

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    this.maxContainerRam = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT,
        Long.toString(instanceRamDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.maxContainerCpu = Double.parseDouble(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT,
        Double.toString(instanceCpuDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));

    this.maxContainerDisk = Long.parseLong(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT,
        Long.toString(instanceDiskDefault * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER)));
    createFullTopologyGraph();
  }

  @Override
  public PackingPlan pack() {
    int adjustments = this.numAdjustments;
    // Get the instances using a resource compliant round robin allocation
    Map<String, List<String>> graphPartitioningAllocation
        = getGraphPartitioningAllocation();

    while (graphPartitioningAllocation == null) {
      if (this.numAdjustments > adjustments) {
        adjustments++;
        graphPartitioningAllocation = getGraphPartitioningAllocation();
      } else {
        return null;
      }
    }
    // Construct the PackingPlan
    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    int paddingPercentage = Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
        Integer.toString(DEFAULT_CONTAINER_PADDING_PERCENTAGE)));

    long topologyRam = 0;
    long topologyDisk = 0;
    double topologyCpu = 0.0;

    for (Map.Entry<String, List<String>> entry : graphPartitioningAllocation.entrySet()) {

      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      long containerRam = 0;
      long containerDiskInBytes = 0;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Map<String, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();

      for (String instanceId : instanceList) {
        long instanceRam = 0;
        if (ramMap.containsKey(getComponentName(instanceId))) {
          instanceRam = ramMap.get(getComponentName(instanceId));
        } else {
          instanceRam = instanceRamDefault;
        }
        containerRam += instanceRam;

        // Currently not yet support disk or cpu config for different components,
        // so just use the default value.
        long instanceDisk = instanceDiskDefault;
        containerDiskInBytes += instanceDisk;

        double instanceCpu = instanceCpuDefault;
        containerCpu += instanceCpu;

        Resource resource =
            new Resource(instanceCpu, instanceRam, instanceDisk);
        PackingPlan.InstancePlan instancePlan =
            new PackingPlan.InstancePlan(
                instanceId,
                getComponentName(instanceId),
                resource);
        // Insert it into the map
        instancePlanMap.put(instanceId, instancePlan);
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam += (paddingPercentage * containerRam) / 100;
      containerDiskInBytes += (paddingPercentage * containerDiskInBytes) / 100;

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlanMap, resource);

      containerPlanMap.put(containerId, containerPlan);
      topologyRam += containerRam;
      topologyCpu += Math.round(containerCpu);
      topologyDisk += containerDiskInBytes;
    }

    // Take the heron internal container into account and the application master for YARN
    // scheduler
    topologyRam += instanceRamDefault;
    topologyDisk += instanceDiskDefault;
    topologyCpu += instanceCpuDefault;

    Resource resource = new Resource(
        topologyCpu, topologyRam, topologyDisk);

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlanMap, resource);

    return plan;
  }

  @Override
  public void close() {

  }

  private TopologyGraph createFullTopologyGraph() {
    TopologyGraph tGraph = new TopologyGraph(topology.getName(), 3);
    int numVertices = 0;
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    for (TopologyAPI.Spout.Builder spout : topology.toBuilder().getSpoutsBuilderList()) {
      String component = spout.getComp().getName();
      int cnt = createVertices(tGraph, numVertices, parallelismMap, ramMap, component, 0);
      numVertices += cnt;
      System.out.println(component + " " + 0 + " " + cnt);
    }
    int level = 0;
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String component = bolt.getComp().getName();
      level++;
      int cnt = createVertices(tGraph, numVertices, parallelismMap, ramMap, component, level);
      numVertices += cnt;
      System.out.println(component + " " + level + " " + cnt);
    }
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String name = bolt.getComp().getName();
      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        System.out.println(name + " " + parent + " " + inputStream.getGtype());
        //connect instances of parents with instances of children
        ArrayList<Integer> parentVertices = componentVertices.get(parent);
        ArrayList<Integer> childVertices = componentVertices.get(name);
        connectVertices(tGraph, parentVertices, childVertices, inputStream.getGtype());
      }
    }
    return tGraph;
  }

  private void connectVertices(TopologyGraph tGraph,
                               ArrayList<Integer> parent, ArrayList<Integer> child,
                               TopologyAPI.Grouping groupingType) {
    if (groupingType.getNumber() == 1) { //shuffle grouping
      for (int i = 0; i < parent.size(); i++) {
        for (int j = 0; j < child.size(); j++) {
          tGraph.addEdge(new Edge(parent.get(i), child.get(j), 1));
          System.out.println("edge " + " " + parent.get(i) + " " + child.get(j));
        }
      }
    }
  }

  private int createVertices(TopologyGraph tGraph, int numVertices,
                             Map<String, Integer> parallelismMap,
                             Map<String, Long> ramMap,
                             String component, int level) {
    int cnt = numVertices;
    ArrayList<Integer> vertices = new ArrayList<Integer>();
    for (int i = 0; i < parallelismMap.get(component); i++) {
      cnt++;
      Vertex v;
      Resource rs = getComponentResources(component, ramMap);
      if (!isValidInstance(rs)) {
        throw new RuntimeException("The topology configuration does not have "
            + "valid resource requirements. Please make sure that the instance resource "
            + "requirements do not exceed the maximum per-container resources.");
      } else {
        v = new Vertex(cnt, level, rs, component);
      }
      vertices.add(cnt);
      tGraph.addVertex(v);
    }
    componentVertices.put(component, vertices);
    return cnt - numVertices;
  }

  public Resource getComponentResources(String component, Map<String, Long> ramMap) {
    Resource rs;
    if (ramMap.containsKey(component)) {
      rs = new Resource(instanceCpuDefault, ramMap.get(component), instanceDiskDefault);
    } else {
      rs = new Resource(instanceCpuDefault, instanceRamDefault, instanceDiskDefault);
    }
    return rs;
  }

  public void evaluatePackingPlan(TopologyGraph t, int noContainers) {
    String file = System.getProperty("user.dir") + "/" + t.getName() + ".part." + noContainers;
    HashMap<String, Resource> plan = new HashMap<String, Resource>();
    try {
      FileReader inFile = new FileReader(file);
      BufferedReader br = new BufferedReader(inFile);
      String s;
      int lineNo = 1;
      while ((s = br.readLine()) != null) {
        int containerId = Integer.parseInt(s);
        String component = t.getVertex(lineNo).getComponent();
        if (plan.containsKey(component)) {
          Resource r = plan.get(component);
          //continue from here
        }
        System.out.println(containerId + " " + t.getVertex(lineNo).getComponent());
        //this.getPlan()[containerId].addInstance(t.getVertex(lineNo));
        lineNo++;
      }
      inFile.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Get the instances' allocation based on the First Fit Decreasing algorithm
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  protected Map<String, List<String>> getGraphPartitioningAllocation() {
    Map<String, List<String>> allocation = new HashMap<>();
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    //create graph
    TopologyGraph tGraph = createFullTopologyGraph();
    //create metis file
    tGraph.createMetisfile();
    //invoke library
    String cmd = null;
    int noContainers = 2;
    cmd = "/home/avrilia/graphs/metis/bin/./gpmetis "
        + System.getProperty("user.dir") + "/" + tGraph.getName() + " " + noContainers;
    System.out.println("LLL " + cmd);
    try {
      final Process p = Runtime.getRuntime().exec(cmd);
      p.waitFor();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
    evaluatePackingPlan(tGraph, 2);
    //create and validate packingplan
    //start over if needed.
   /* ArrayList<RamRequirement> ramRequirements = getSortedRAMInstances();

    if (ramRequirements == null) {
      return null;
    }
    for (int i = 0; i < ramRequirements.size(); i++) {
      String component = ramRequirements.get(i).getComponentName();
      int numInstance = parallelismMap.get(component);
      for (int j = 0; j < numInstance; j++) {
        int containerId = placeFFDInstance(containers,
            ramRequirements.get(i).getRamRequirement(),
            instanceCpuDefault, instanceDiskDefault);
        if (allocation.containsKey(getContainerId(containerId))) {
          allocation.get(getContainerId(containerId)).
              add(getInstanceId(containerId, component, globalTaskIndex, j));
        } else {
          ArrayList<String> instance = new ArrayList<>();
          instance.add(getInstanceId(containerId, component, globalTaskIndex, j));
          allocation.put(getContainerId(containerId), instance);
        }
        globalTaskIndex++;
      }
    }*/
    return allocation;
  }

  /**
   * Check whether the Instance has enough RAM and whether it can fit within the container limits.
   *
   * @param instanceResources The resources allocated to the instance
   * @return true if the instance is valid, false otherwise
   */
  protected boolean isValidInstance(Resource instanceResources) {

    if (instanceResources.getRam() < MIN_RAM_PER_INSTANCE) {
      LOG.severe(String.format(
          "Require at least %d MB ram per instance",
          MIN_RAM_PER_INSTANCE / Constants.MB));
      return false;
    }

    if (instanceResources.getRam() > maxContainerRam) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB ram. The current max container"
              + "size is %d MB",
          instanceResources.getRam(), maxContainerRam));
      return false;
    }

    if (instanceResources.getCpu() > maxContainerCpu) {
      LOG.severe(String.format(
          "This instance requires containers with at least %d cpu cores. The current max container"
              + "size is %d cores",
          instanceResources.getCpu(), maxContainerCpu));
      return false;
    }

    if (instanceResources.getDisk() > maxContainerDisk) {
      LOG.severe(String.format(
          "This instance requires containers of at least %d MB disk. The current max container"
              + "size is %d MB",
          instanceResources.getDisk(), maxContainerDisk));
      return false;
    }
    return true;
  }
}
