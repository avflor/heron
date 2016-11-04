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
package com.twitter.heron.spi.slamgr;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;

public interface SLAPolicy extends AutoCloseable {

  /**
   * Initialize the SLA policy
   */
  void initialize(Config conf, TopologyAPI.Topology topology, SinkVisitor visitor);

  /**
   * It executes a set of detectors and resolvers. This method implements all the logic
   * for combining detectors and resolvers to identify and solve a specific problem
   */
  void execute();

  /**
   * Close the SLA policy
   */
  void close();
}
