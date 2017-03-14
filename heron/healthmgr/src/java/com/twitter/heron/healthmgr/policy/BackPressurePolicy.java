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

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.actionlog.ActionEntry;
import com.twitter.heron.healthmgr.diagnoser.DataSkewDiagnoser;
import com.twitter.heron.healthmgr.diagnoser.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.diagnoser.UnderProvisioningDiagnoser;
import com.twitter.heron.healthmgr.resolver.ScaleUpResolver;
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


public class BackPressurePolicy implements HealthPolicy {

  private UnderProvisioningDiagnoser underProvisioningDiagnoser = new UnderProvisioningDiagnoser();
  private DataSkewDiagnoser dataSkewDiagnoser = new DataSkewDiagnoser();
  private SlowInstanceDiagnoser slowInstanceDiagnoser = new SlowInstanceDiagnoser();

  private ScaleUpResolver scaleUpResolver = new ScaleUpResolver();
  private TopologyAPI.Topology topology;

  private DiagnoserService diagnoserService;
  private ResolverService resolverService;

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);

    underProvisioningDiagnoser.initialize(conf, runtime);
    dataSkewDiagnoser.initialize(conf, runtime);
    slowInstanceDiagnoser.initialize(conf, runtime);

    scaleUpResolver.initialize(conf, runtime);
    diagnoserService = (DiagnoserService) Runtime
        .getDiagnoserService(runtime);
    resolverService = (ResolverService) Runtime
        .getResolverService(runtime);
  }

  @Override
  public void execute() {

    Diagnosis<ComponentSymptom> slowInstanceDiagnosis =
        diagnoserService.run(slowInstanceDiagnoser, topology);

    if (slowInstanceDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "SLOW_INSTANCE_RESOLVER",
          slowInstanceDiagnosis, slowInstanceDiagnoser)) {
      }
    }

    Diagnosis<ComponentSymptom> dataSkewDiagnosis =
        diagnoserService.run(dataSkewDiagnoser, topology);

    if (dataSkewDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "DATA_SKEW_RESOLVER",
          dataSkewDiagnosis, dataSkewDiagnoser)) {

      }
    }

    Diagnosis<ComponentSymptom> limitedParallelismDiagnosis =
        diagnoserService.run(underProvisioningDiagnoser, topology);

    if (limitedParallelismDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "SCALE_UP_RESOLVER",
          limitedParallelismDiagnosis, underProvisioningDiagnoser)) {
        double outcomeImprovement = resolverService.estimateResolverOutcome(scaleUpResolver,
            topology, limitedParallelismDiagnosis);
        resolverService.run(scaleUpResolver, topology, "SCALE_UP_RESOLVER",
            limitedParallelismDiagnosis, outcomeImprovement);
      }
    }
  }

  @Override
  public void evaluate() {
    ActionEntry<? extends Symptom> lastAction = resolverService.getLog()
        .getLastAction(topology.getName());
    System.out.println("last action " + lastAction);
    if (lastAction != null) {
      switch (lastAction.getAction()) {
        case "DATA_SKEW_RESOLVER": {
          evaluateAction(dataSkewDiagnoser, null, lastAction);
          break;
        }
        case "SLOW_INSTANCE_RESOLVER":
          evaluateAction(slowInstanceDiagnoser, null, lastAction);
          break;
        case "SCALE_UP_RESOLVER":
          evaluateAction(underProvisioningDiagnoser, scaleUpResolver, lastAction);
          break;
        default:
          break;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Symptom> void evaluateAction(IDiagnoser<T> diagnoser, IResolver<T> resolver,
                                                  ActionEntry<? extends Symptom> lastAction) {
    Boolean success = true;
    Diagnosis<? extends Symptom> newDiagnosis;
    newDiagnosis = diagnoserService.run(diagnoser, topology);
    if (newDiagnosis != null) {
      success = resolverService.isSuccessfulAction(resolver,
          ((ActionEntry<T>) lastAction).getDiagnosis(), (Diagnosis<T>) newDiagnosis,
          ((ActionEntry<T>) lastAction).getChange());
      System.out.println("evaluating" + success);
      if (!success) {
        System.out.println("bad action");
        resolverService.addToBlackList(topology, lastAction.getAction(), lastAction.getDiagnosis(),
            ((ActionEntry<T>) lastAction).getChange());
      }
    }
  }

  @Override
  public void close() {
    underProvisioningDiagnoser.close();
    dataSkewDiagnoser.close();
    slowInstanceDiagnoser.close();
    scaleUpResolver.close();
  }
}
