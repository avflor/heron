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

package com.twitter.heron.healthmgr.services;


import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.actionlog.ActionBlackList;
import com.twitter.heron.healthmgr.actionlog.ActionEntry;
import com.twitter.heron.healthmgr.actionlog.ActionLog;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDiagnoser;
import com.twitter.heron.spi.healthmgr.IResolver;
import com.twitter.heron.spi.healthmgr.Symptom;

public class ResolverService {

  private ActionLog log;
  private ActionBlackList blacklist;

  public ResolverService() {

    this.log = new ActionLog();
    this.blacklist = new ActionBlackList();
  }

  public void initialize(Config config, Config runtime) {
  }


  public <T extends Symptom> double estimateResolverOutcome(IResolver<T> resolver,
                                                            TopologyAPI.Topology topology,
                                                            Diagnosis<T> diagnosis) {
    return resolver.estimateOutcome(diagnosis, topology);
  }


  public <T extends Symptom> boolean run(IResolver<T> resolver, TopologyAPI.Topology topology,
                                         String problem, Diagnosis<T> diagnosis,
                                         double outcome) {

    log.addAction(topology.getName(), problem, diagnosis, outcome);
    return resolver.resolve(diagnosis, topology);
  }

  public <T extends Symptom> void addToBlackList(TopologyAPI.Topology topology,
                                                 String actionTaken,
                                                 Diagnosis<T> data, double change) {
    blacklist.addToBlackList(topology.getName(), actionTaken, data, change);
    System.out.println("blacklist " + blacklist.toString());
  }

  public <T extends Symptom> boolean isSuccessfulAction(IResolver<T> resolver,
                                                       Diagnosis<T> firstDiagnosis,
                                                       Diagnosis<T> secondDiagnosis,
                                                       double change) {
    return resolver.successfulAction(firstDiagnosis, secondDiagnosis, change);
  }

  @SuppressWarnings("unchecked")
  public <T extends Symptom> boolean isBlackListedAction(TopologyAPI.Topology topology,
                                                         String action,
                                                         Diagnosis<T> diagnosis,
                                                         IDiagnoser<T> diagnoser) {
    if (blacklist.existsBlackList(topology.getName())) {
      for (ActionEntry<? extends Symptom> actionEntry : blacklist.getTopologyBlackList(
          topology.getName())) {
        if (action.equals(actionEntry.getAction()) &&
            diagnoser.similarDiagnosis(((ActionEntry<T>) actionEntry).getDiagnosis(), diagnosis)) {
          return true;
        }
      }
    }
    return false;
  }

  public ActionLog getLog() {
    return this.log;
  }

  public ActionBlackList getBlacklist() {
    return blacklist;
  }

  public void close() {

  }
}
