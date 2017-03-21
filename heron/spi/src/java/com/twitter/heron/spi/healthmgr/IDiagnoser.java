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
package com.twitter.heron.spi.healthmgr;


import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;

public interface IDiagnoser<T extends Symptom> extends AutoCloseable {

  /**
   * Initialize the detector with the config and a sink visitor
   */
  void initialize(Config config, Config runtime);

  /**
   * Called by detector to detect a potential problem.
   */
  Diagnosis<T> diagnose(TopologyAPI.Topology topology);

  /**
   * Called by detector to compare two Diagnosis objects.
   * Returns false if the objects are considered different, true otherwise
   */
  boolean similarDiagnosis(Diagnosis<T> firstDiagnosis, Diagnosis<T> secondDiagnosis);

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the detector.
   */
  void close();
}
