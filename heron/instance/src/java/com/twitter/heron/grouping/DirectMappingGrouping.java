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
package com.twitter.heron.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.TopologyContext;

public class DirectMappingGrouping implements CustomStreamGrouping {

  private static final Logger LOG = Logger.getLogger(DirectMappingGrouping.class.getName());
  private static final long serialVersionUID = 2281355231811468414L;
  private int taskIdIndex;
  private int noSourceTasks;
  private List<Integer> groupingTargetTasks;
  private int lastIndex;

  public DirectMappingGrouping() {
    groupingTargetTasks = new ArrayList<Integer>();
  }

  @Override
  public void prepare(TopologyContext context, String component,
                      String streamId, List<Integer> targetTasks) {
    this.taskIdIndex = context.getThisTaskIndex();
    this.noSourceTasks = context.getComponentTasks(component).size();
    this.groupingTargetTasks = targetTasks;
    LOG.info("Number of target tasks: " + targetTasks.size() + ", "
        + "Number of source tasks: " + context.getComponentTasks(component).size() + ", "
        + "Index of current task: " + context.getThisTaskIndex());
    this.lastIndex = -1;

  }

  @Override
  public List<Integer> chooseTasks(List<Object> values) {
    List<Integer> res = new ArrayList<Integer>();
    float ratio = noSourceTasks / (float) groupingTargetTasks.size();
    //System.out.println("Ratio " + ratio);
    if (ratio >= 1) {
      // ratio sources connect to 1 destination
      int dest = this.taskIdIndex / (int) ratio;
      res.add(groupingTargetTasks.get(dest));
      // System.out.println(this.taskIdIndex + " " + dest);
      // System.out.println(targetTasks.get(dest));
    } else {
      //every source to 1/ratio destinations
      int numDest = (int) (1 / ratio);
      int dest = this.taskIdIndex * numDest;
      //  System.out.println(this.taskIdIndex + " " + numDest + " " + dest);
      if (lastIndex == -1 || lastIndex == dest + numDest - 1) {
        lastIndex = dest;
      } else {
        lastIndex++;
      }
      //  System.out.println("LLL " + targetTasks.get(lastIndex));
      res.add(groupingTargetTasks.get(lastIndex));

    }
    return res;
  }
}
