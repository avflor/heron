//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.healthmgr.policy;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.actionlog.ActionEntry;
import com.twitter.heron.healthmgr.diagnoser.DataSkewDiagnoser;
import com.twitter.heron.healthmgr.diagnoser.OverprovisioningDiagnoser;
import com.twitter.heron.healthmgr.diagnoser.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.diagnoser.UnderProvisioningDiagnoser;
import com.twitter.heron.healthmgr.resolver.ScaleDownResolver;
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

public class DynamicResourceAllocationPolicy implements HealthPolicy {

  private UnderProvisioningDiagnoser underProvisioningDiagnoser = new UnderProvisioningDiagnoser();
  private DataSkewDiagnoser dataSkewDiagnoser = new DataSkewDiagnoser();
  private SlowInstanceDiagnoser slowInstanceDiagnoser = new SlowInstanceDiagnoser();
  private OverprovisioningDiagnoser overprovisioningDiagnoser = new OverprovisioningDiagnoser();

  private ScaleDownResolver scaleDownResolver = new ScaleDownResolver();
  private ScaleUpResolver scaleUpResolver = new ScaleUpResolver();
  private TopologyAPI.Topology topology;

  private DiagnoserService diagnoserService;
  private ResolverService resolverService;


  public void setPacketsThreshold(int noPackets) {
    overprovisioningDiagnoser.setPacketThreshold(noPackets);
  }

  @Override
  public void initialize(Config conf, Config runtime) {
    this.topology = Runtime.topology(runtime);

    underProvisioningDiagnoser.initialize(conf, runtime);
    dataSkewDiagnoser.initialize(conf, runtime);
    slowInstanceDiagnoser.initialize(conf, runtime);
    overprovisioningDiagnoser.initialize(conf, runtime);

    scaleDownResolver.initialize(conf, runtime);
    scaleUpResolver.initialize(conf, runtime);

    diagnoserService = (DiagnoserService) Runtime
        .getDiagnoserService(runtime);
    resolverService = (ResolverService) Runtime
        .getResolverService(runtime);
  }

  @Override
  public void execute() {

    Diagnosis<ComponentSymptom> overprovisioningDiagnosis =
        diagnoserService.run(overprovisioningDiagnoser, topology);

    if (overprovisioningDiagnosis != null && overprovisioningDiagnosis.getSummary().size() != 0) {
      System.out.println(overprovisioningDiagnosis.getSummary().toString());
      Diagnosis<ComponentSymptom> lowPendingPacketsDiagnosis = new Diagnosis<>();
      lowPendingPacketsDiagnosis.addToDiagnosis(overprovisioningDiagnosis.getSummary().
          iterator().next());
      if (!resolverService.isBlackListedAction(topology, "SCALE_DOWN_RESOLVER",
          lowPendingPacketsDiagnosis, overprovisioningDiagnoser)) {
        double outcomeImprovement = resolverService.estimateResolverOutcome(scaleDownResolver,
            topology, lowPendingPacketsDiagnosis);
        resolverService.run(scaleDownResolver, topology, "SCALE_DOWN_RESOLVER",
            lowPendingPacketsDiagnosis, outcomeImprovement);
      }
      return;
    }

    Diagnosis<ComponentSymptom> slowInstanceDiagnosis =
        diagnoserService.run(slowInstanceDiagnoser, topology);

    if (slowInstanceDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "SLOW_INSTANCE_RESOLVER",
          slowInstanceDiagnosis, slowInstanceDiagnoser)) {
      }
      return;
    }

    Diagnosis<ComponentSymptom> dataSkewDiagnosis =
        diagnoserService.run(dataSkewDiagnoser, topology);

    if (dataSkewDiagnosis != null) {
      if (!resolverService.isBlackListedAction(topology, "DATA_SKEW_RESOLVER",
          dataSkewDiagnosis, dataSkewDiagnoser)) {

      }
      return;
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
      return;
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
        case "SCALE_DOWN_RESOLVER": {
          evaluateAction(overprovisioningDiagnoser, scaleDownResolver, lastAction);
          break;
        }
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
    overprovisioningDiagnoser.close();
    scaleDownResolver.close();
  }
}
