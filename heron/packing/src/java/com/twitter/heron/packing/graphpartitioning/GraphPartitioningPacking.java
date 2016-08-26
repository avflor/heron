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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.packing.binpacking.FirstFitDecreasingPacking;
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
  protected int noContainers = 0;
  protected String path;
  protected PackingPlan ffdplan;

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
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
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

    FirstFitDecreasingPacking ffdpacking = new FirstFitDecreasingPacking();
    ffdpacking.initialize(config, topology);
    ffdplan = ffdpacking.pack();
    this.noContainers = ffdplan.getContainers().size();

    this.path = System.getProperty("java.io.tmpdir");
    System.out.println("OOO " + path);

  }

  @Override
  public PackingPlan pack() {
    if (this.noContainers == 1) {
      return ffdplan;
    }
    // Get the instances using a resource compliant round robin allocation
    Map<Integer, List<String>> graphPartitioningAllocation
        = getGraphPartitioningAllocation();

    while (graphPartitioningAllocation == null) {
      noContainers++;
      graphPartitioningAllocation = getGraphPartitioningAllocation();
    }
    // Construct the PackingPlan
    Set<PackingPlan.ContainerPlan> containerPlans = new HashSet<>();
    Map<String, Long> ramMap = TopologyUtils.getComponentRamMapConfig(topology);

    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();
    int paddingPercentage = Integer.parseInt(TopologyUtils.getConfigWithDefault(
        topologyConfig, com.twitter.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
        Integer.toString(DEFAULT_CONTAINER_PADDING_PERCENTAGE)));

    long topologyRam = 0;
    long topologyDisk = 0;
    double topologyCpu = 0.0;

    for (Map.Entry<Integer, List<String>> entry : graphPartitioningAllocation.entrySet()) {

      int containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      long containerRam = 0;
      long containerDiskInBytes = 0;
      double containerCpu = 0;

      // Calculate the resource required for single instance
      Set<PackingPlan.InstancePlan> instancePlans = new HashSet<>();

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
        instancePlans.add(instancePlan);
      }

      containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam += (paddingPercentage * containerRam) / 100;
      containerDiskInBytes += (paddingPercentage * containerDiskInBytes) / 100;

      Resource resource =
          new Resource(Math.round(containerCpu), containerRam, containerDiskInBytes);

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlans, resource);

      containerPlans.add(containerPlan);
      topologyRam += containerRam;
      topologyCpu += Math.round(containerCpu);
      topologyDisk += containerDiskInBytes;
    }

    PackingPlan plan = new PackingPlan(topology.getId(), containerPlans);

    LOG.info("Created a packing plan with " + containerPlans.size() + " containers");
    Object[] containerPlansArray = containerPlans.toArray();
    for (int i = 0; i < containerPlans.size(); i++) {
      LOG.info("Container  " + ((PackingPlan.ContainerPlan) containerPlansArray[i]).getId()
          + " consists of "
          + ((PackingPlan.ContainerPlan) containerPlansArray[i]).getInstances().toString());
    }
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
      //System.out.println(component + " " + 0 + " " + cnt);
    }
    int level = 0;
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String component = bolt.getComp().getName();
      level++;
      int cnt = createVertices(tGraph, numVertices, parallelismMap, ramMap, component, level);
      numVertices += cnt;
      //System.out.println(component + " " + level + " " + cnt);
    }
    for (TopologyAPI.Bolt.Builder bolt : topology.toBuilder().getBoltsBuilderList()) {
      String name = bolt.getComp().getName();
      // To get the parent's component to construct a graph of topology structure
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        String parent = inputStream.getStream().getComponentName();
        //System.out.println(name + " " + parent + " " + inputStream.getGtype());
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
    if (groupingType.getNumber() != 7) {
      for (int i = 0; i < parent.size(); i++) {
        for (int j = 0; j < child.size(); j++) {
          tGraph.addEdge(new Edge(parent.get(i), child.get(j), 1));
          LOG.info("Edge: " + " " + parent.get(i) + " " + child.get(j));
        }
      }
    } else { //custom grouping
      float ratio = parent.size() / (float) child.size();
      if (ratio >= 1) {
        // ratio sources connect to 1 destination
        for (int i = 0; i < parent.size(); i++) {
          int dest = i / (int) ratio;
          tGraph.addEdge(new Edge(parent.get(i), child.get(dest), 1));
          LOG.info("Edge: " + " " + parent.get(i) + " " + child.get(dest));
        }
      } else {
        //every source to 1/ratio destinations
        int numDest = (int) (1 / ratio);
        for (int i = 0; i < parent.size(); i++) {
          int dest = i * numDest;
          for (int j = dest; j <= dest + numDest - 1; j++) {
            tGraph.addEdge(new Edge(parent.get(i), child.get(j), 1));
            LOG.info("Edge: " + " "
                + parent.get(i) + " " + child.get(j) + " " + numDest);
          }
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

  /**
   * Evaluate the results of the mets library
   *
   * @param t TopologyGraph
   * @return Assignment of the instances of each component to the containers
   */

  public HashMap<String, ArrayList<Integer>> evaluateResults(TopologyGraph t) {
    String file = path + "/" + t.getName() + ".part." + noContainers;
    HashMap<Integer, Resource> containerResourceAllocation = new HashMap<Integer, Resource>();
    HashMap<String, ArrayList<Integer>> componentAssignment
        = new HashMap<String, ArrayList<Integer>>();

    try {
      FileReader inFile = new FileReader(file);
      BufferedReader br = new BufferedReader(inFile);
      String s;
      int lineNo = 1;
      while ((s = br.readLine()) != null) {
        int containerId = Integer.parseInt(s);
        Vertex v = t.getVertex(lineNo);

        //Update componentAssignment
        ArrayList<Integer> containers = null;
        if (componentAssignment.containsKey(v.getComponent())) {
          containers = componentAssignment.get(v.getComponent());
        } else {
          containers = new ArrayList<>();
        }
        containers.add(containerId);
        componentAssignment.put(v.getComponent(), containers);

        //Update container resources
        if (containerResourceAllocation.containsKey(containerId)) {
          Resource r = containerResourceAllocation.get(containerId);
          r.setCpu(r.getCpu() + v.getVertexWeights().getCpu());
          r.setRam(r.getRam() + v.getVertexWeights().getRam());
          r.setDisk(r.getDisk() + v.getVertexWeights().getDisk());
          containerResourceAllocation.put(containerId, r);
        } else {
          Resource r = new Resource(v.getVertexWeights().getCpu(), v.getVertexWeights().getRam(),
              v.getVertexWeights().getDisk());
          containerResourceAllocation.put(containerId, r);
        }
        //System.out.println(containerId + " " + t.getVertex(lineNo).getComponent());
        lineNo++;
      }
      inFile.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    String cmd1 = "rm " + path + "/" + t.getName();

    String cmd2 = "rm " + path + "/" + t.getName() + ".part." + noContainers;

    // System.out.println("LLL " + cmd);
    try {
      final Process p1 = Runtime.getRuntime().exec(cmd1);
      p1.waitFor();
      final Process p2 = Runtime.getRuntime().exec(cmd2);
      p2.waitFor();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }

    if (evaluateAllocation(containerResourceAllocation)) {
      return componentAssignment;
    } else {
      return null;
    }
  }

  public boolean evaluateAllocation(HashMap<Integer, Resource> containerResourceAllocation) {
    for (Map.Entry<Integer, Resource> entry : containerResourceAllocation.entrySet()) {
      Integer containerId = entry.getKey();
      Resource r = entry.getValue();
      //System.out.println(containerId + " " + r.getDisk() + " " + r.getCpu() + " " + r.getRam());
      if (!isValidInstance(r)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the instances' allocation based on GraphPartitioning
   *
   * @return Map &lt; containerId, list of InstanceId belonging to this container &gt;
   */
  protected Map<Integer, List<String>> getGraphPartitioningAllocation() {
    Map<Integer, List<String>> allocation = new HashMap<>();
    Map<String, Integer> parallelismMap = TopologyUtils.getComponentParallelism(topology);
    //create graph
    TopologyGraph tGraph = createFullTopologyGraph();
    //create metis file
    tGraph.createMetisfile(path);
    String cmd = null;

    cmd = "/home/avrilia/graphs/metis/bin/./gpmetis  "
        + path + "/" + tGraph.getName() + " " + noContainers;
    System.out.println("LLL " + cmd);
    try {
      final Process p = Runtime.getRuntime().exec(cmd);
      p.waitFor();
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
    //create the packing plan
    HashMap<String, ArrayList<Integer>> componentAssignment = evaluateResults(tGraph);

    if (componentAssignment == null) {
      return null;
    } else {
      int globalTaskIndex = 1;
      for (Map.Entry<String, ArrayList<Integer>> entry : componentAssignment.entrySet()) {
        String component = entry.getKey();
        ArrayList<Integer> containerIds = entry.getValue();

        for (int j = 0; j < containerIds.size(); j++) {
          int containerId = containerIds.get(j) + 1;
          if (allocation.containsKey(containerId)) {
            allocation.get(containerId).
                add(getInstanceId(containerId, component, globalTaskIndex, j));
          } else {
            ArrayList<String> instance = new ArrayList<>();
            instance.add(getInstanceId(containerId, component, globalTaskIndex, j));
            allocation.put(containerId, instance);
          }
          globalTaskIndex++;
        }
      }
      return allocation;
    }
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
