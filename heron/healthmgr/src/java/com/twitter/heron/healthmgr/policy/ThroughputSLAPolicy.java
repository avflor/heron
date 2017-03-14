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

package com.twitter.heron.healthmgr.policy;

import java.util.List;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.actionlog.ActionEntry;
import com.twitter.heron.healthmgr.diagnoser.LowInputThroughputDiagnoser;
import com.twitter.heron.healthmgr.resolver.SpoutScaleUpResolver;
import com.twitter.heron.healthmgr.services.DiagnoserService;
import com.twitter.heron.healthmgr.services.ResolverService;
import com.twitter.heron.scheduler.utils.Runtime;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.ComponentSymptom;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.HealthPolicy;
import com.twitter.heron.spi.healthmgr.IDiagnoser;
import com.twitter.heron.spi.healthmgr.IResolver;
import com.twitter.heron.spi.healthmgr.Symptom;


public class ThroughputSLAPolicy implements HealthPolicy {

  private LowInputThroughputDiagnoser lowInputThroughputDiagnoser =
      new LowInputThroughputDiagnoser();

  private SpoutScaleUpResolver spoutScaleUpResolver = new SpoutScaleUpResolver();
  private TopologyAPI.Topology topology;

  private DiagnoserService diagnoserService;
  private ResolverService resolverService;
  private double maxThroughput = 0;

  private boolean performedAction = false;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);

    lowInputThroughputDiagnoser.initialize(conf, runtime);
    lowInputThroughputDiagnoser.setSpoutThroughput(this.maxThroughput);
    spoutScaleUpResolver.initialize(conf, runtime);
    diagnoserService = (DiagnoserService) Runtime
        .getDiagnoserService(runtime);
    resolverService = (ResolverService) Runtime
        .getResolverService(runtime);
  }

  public void setSpoutThroughput(double throughput) {
    this.maxThroughput = throughput;
  }

  @Override
  public void execute() {

    Diagnosis<ComponentSymptom> lowInputThroughputDiagnosis =
        diagnoserService.run(lowInputThroughputDiagnoser, topology);
    if (lowInputThroughputDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "SPOUT_SCALE_UP_RESOLVER",
          lowInputThroughputDiagnosis, lowInputThroughputDiagnoser)) {
        spoutScaleUpResolver.setMaxEmitCount(this.maxThroughput);
        double outcomeImprovement = resolverService.estimateResolverOutcome(
            spoutScaleUpResolver, topology, lowInputThroughputDiagnosis);
        resolverService.run(spoutScaleUpResolver, topology, "SPOUT_SCALE_UP_RESOLVER",
            lowInputThroughputDiagnosis, outcomeImprovement);
      }
    }
  }

  private int contains(List<TopologyAPI.Spout> spouts, String name) {
    for (int i = 0; i < spouts.size(); i++) {
      if (spouts.get(i).getComp().getName().equals(name)) {
        return i;
      }
    }
    return -1;
  }


  @Override
  public void evaluate() {
    /*if(performedAction) {
      ActionEntry<? extends Symptom> lastAction = resolverService.getLog()
          .getLastAction(topology.getName());
      System.out.println("last action " + lastAction);
      evaluateAction(emitCountDetector, spoutScaleUpResolver, lastAction);
    }*/
  }

  @SuppressWarnings("unchecked")
  private <T extends Symptom> void evaluateAction(IDiagnoser<T> detector, IResolver<T> resolver,
                                                  ActionEntry<? extends Symptom> lastAction) {
    return;
   /* Boolean success = true;
    Diagnosis<? extends Symptom> newDiagnosis;
    newDiagnosis = symptomDetectorService.run(detector, topology);
    List<TopologyAPI.Spout> spouts = topology.getSpoutsList();
    Diagnosis<ComponentSymptom> spoutDiagnosis = new Diagnosis<>();

    for(ComponentSymptom component : ((Diagnosis<ComponentSymptom>) newDiagnosis).getSummary()){
      int position = contains(spouts, component.getComponentName());
      if(position != -1){
        spoutDiagnosis.addToDiagnosis(component);
      }
    }
    if (newDiagnosis != null) {
      success = resolverService.isSuccesfulAction(resolver,
          ((ActionEntry<T>) lastAction).getDiagnosis(), (Diagnosis<T>) spoutDiagnosis,
          ((ActionEntry<T>) lastAction).getChange());
      System.out.println("evaluating " + success);
      if (!success) {
        System.out.println("bad action");
        resolverService.addToBlackList(topology, lastAction.getAction(), lastAction.getDiagnosis(),
            ((ActionEntry<T>) lastAction).getChange());
      }
    }*/
  }

  @Override
  public void close() {
    lowInputThroughputDiagnoser.close();
    spoutScaleUpResolver.close();
  }
}
