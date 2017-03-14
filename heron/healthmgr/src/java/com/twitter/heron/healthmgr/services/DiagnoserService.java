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
package com.twitter.heron.healthmgr.services;

import java.util.Set;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.healthmgr.Diagnosis;
import com.twitter.heron.spi.healthmgr.IDiagnoser;
import com.twitter.heron.spi.healthmgr.Symptom;

public class DiagnoserService {

  public DiagnoserService() {
  }

  public void initialize(Config config, Config runtime) {
  }

  public <T extends Symptom> Diagnosis<T> run(IDiagnoser<T> diagnoser,
                                              TopologyAPI.Topology topology) {
    return diagnoser.diagnose(topology);
  }

  public <T extends Symptom> boolean similarDiagnosis(IDiagnoser<T> diagnoser,
                                                         Diagnosis<T> firstDiagnosis,
                                                         Diagnosis<T> secondDiagnosis) {
    return diagnoser.similarDiagnosis(firstDiagnosis, secondDiagnosis);
  }


  public void close() {
  }
}
