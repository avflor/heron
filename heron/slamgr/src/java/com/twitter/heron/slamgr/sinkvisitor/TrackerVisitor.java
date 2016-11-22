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


package com.twitter.heron.slamgr.sinkvisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;

public class TrackerVisitor implements SinkVisitor {

  private WebTarget target;
  private List<TopologyAPI.Bolt> bolts;

  @Override
  public void initialize(Config conf, TopologyAPI.Topology topology) {
    this.bolts = topology.getBoltsList();
    Client client = ClientBuilder.newClient();
    this.target = client.target("http://localhost:8888/topologies/metrics")
        .queryParam("cluster", "local")
        .queryParam("environ", "default")
        .queryParam("topology", topology.getName())
        .queryParam("interval", "60");
  }

  @Override
  public Iterable<MetricsInfo> getNextMetric(String metric, String... component) {
    List<MetricsInfo> metricsInfo = new ArrayList<MetricsInfo>();
    for (int j = 0; j < component.length; j++) {
      target = target.queryParam("metricname", metric)
          .queryParam("component", component[j]);
      Response r = target.request(MediaType.APPLICATION_JSON_TYPE).get();
      TrackerOutput result = r.readEntity(TrackerOutput.class);
      List<MetricsInfo> tmp = convert(result, metric);
      metricsInfo.addAll(tmp);
    }
    return metricsInfo;
  }

  @Override
  public void close() {
  }

  private List<MetricsInfo> convert(TrackerOutput output, String metricName) {
    List<MetricsInfo> metricsInfo = new ArrayList<MetricsInfo>();
    Map<String, String> instanceData = output.getResult().getMetrics().get(metricName);
    for (String instanceName : instanceData.keySet()) {
      metricsInfo.add(new MetricsInfo(instanceName, instanceData.get(instanceName)));
    }
    return metricsInfo;
  }

}
