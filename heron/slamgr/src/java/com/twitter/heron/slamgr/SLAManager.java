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

package com.twitter.heron.slamgr;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.slamgr.policy.FailedTuplesPolicy;
import com.twitter.heron.slamgr.sinkvisitor.TrackerVisitor;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.metricsmgr.sink.SinkVisitor;
import com.twitter.heron.spi.slamgr.SLAPolicy;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.LauncherUtils;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.Shutdown;

/**
 * e.g. options
 * -d ~/.heron -p ~/.heron/conf/local -c local -e default -r userName -t AckingTopology
 */
public class SLAManager {
  private static final Logger LOG = Logger.getLogger(SLAManager.class.getName());
  private static String topologyName;
  private static Properties schedulerProperties;
  private final Config config;
  private Config runtime;
  private SLAPolicy policy;
  private SinkVisitor sinkVisitor;
  private ScheduledExecutorService executor;
  private TopologyAPI.Topology topology;

  public SLAManager(Config config) {
    this.config = config;
  }

  /**
   * Load the defaults config
   *
   * @param heronHome, directory of heron home
   * @param configPath, directory containing the config
   * @param releaseFile, release file containing build information
   * <p>
   * return config, the defaults config
   */
  protected static Config defaultConfigs(String heronHome, String configPath, String releaseFile) {
    Config config = Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile))
        .build();
    return config;
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @param verbose, enable verbose logging
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             Boolean verbose) {
    Config config = Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.verbose(), verbose)
        .build();

    return config;
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(SLAManager.class.getSimpleName(), options);
  }

  // Construct all required command line options
  private static Options constructCliOptions() {
    Options options = new Options();

    Option cluster = Option.builder("c")
        .desc("Cluster name in which the topology needs to run on")
        .longOpt("cluster")
        .hasArgs()
        .argName("cluster")
        .required()
        .build();

    Option role = Option.builder("r")
        .desc("Role under which the topology needs to run")
        .longOpt("role")
        .hasArgs()
        .argName("role")
        .required()
        .build();

    Option environment = Option.builder("e")
        .desc("Environment under which the topology needs to run")
        .longOpt("environment")
        .hasArgs()
        .argName("environment")
        .required()
        .build();

    Option heronHome = Option.builder("d")
        .desc("Directory where heron is installed")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home dir")
        .required()
        .build();

    Option configFile = Option.builder("p")
        .desc("Path of the config files")
        .longOpt("config_path")
        .hasArgs()
        .argName("config path")
        .required()
        .build();

    Option topologyNameOption = Option.builder("t")
        .desc("Topology name")
        .longOpt("topology_name")
        .hasArgs()
        .argName("topology name")
        .required()
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(cluster);
    options.addOption(role);
    options.addOption(environment);
    options.addOption(heronHome);
    options.addOption(configFile);
    options.addOption(topologyNameOption);
    options.addOption(verbose);

    return options;
  }

  // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    Options slaManagerCliOptions = constructCliOptions();

    // parse the help options first.
    Options helpOptions = constructHelpOptions();
    CommandLine cmd = parser.parse(helpOptions, args, true);
    if (cmd.hasOption("h")) {
      usage(slaManagerCliOptions);
      return;
    }

    try {
      cmd = parser.parse(slaManagerCliOptions, args);
    } catch (ParseException e) {
      usage(slaManagerCliOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    Boolean verbose = false;
    Level logLevel = Level.INFO;
    if (cmd.hasOption("v")) {
      logLevel = Level.ALL;
      verbose = true;
    }

    // init log
    LoggingHelper.loggerInit(logLevel, false);

    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environ = cmd.getOptionValue("environment");
    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    topologyName = cmd.getOptionValue("topology_name");

    // It returns a new empty Properties instead of null,
    // if no properties passed from command line. So no need for null check.
    schedulerProperties =
        cmd.getOptionProperties(Keys.SCHEDULER_COMMAND_LINE_PROPERTIES_OVERRIDE_OPTION);


    // first load the defaults, then the config from files to override it
    // next add config parameters from the command line
    // load the topology configs

    // build the final config by expanding all the variables
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(defaultConfigs(heronHome, configPath, null))
            .putAll(commandLineConfigs(cluster, role, environ, verbose))
            .put(Keys.topologyName(), topologyName)
            .build());


    LOG.info("Static config loaded successfully ");
    LOG.fine(config.toString());

    SLAManager slaManager = new SLAManager(config);
    slaManager.initialize();

    ScheduledFuture<?> future = slaManager.start();
    try {
      future.get();
    } catch (Exception e) {
      slaManager.executor.shutdownNow();
      throw e;
    }
  }

  private void initialize() throws ReflectiveOperationException {
    getTopologyFromStateManager();
    getRuntime();
    sinkVisitor = new TrackerVisitor();
    sinkVisitor.initialize(config, topology);
    policy = new FailedTuplesPolicy();
    policy.initialize(config, runtime, topology, sinkVisitor);
  }

  private void getTopologyFromStateManager() throws ReflectiveOperationException {
    LOG.log(Level.INFO, "Fetching topology from state manager: {0}", topologyName);
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = ReflectionUtils.newInstance(statemgrClass);
    statemgr.initialize(config);
    SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);
    topology = adaptor.getTopology(topologyName);
    if (topology == null) {
      throw new RuntimeException(String.format("Failed to fetch topology: %s", topologyName));
    }

  }

  private void getRuntime() throws ReflectiveOperationException {
    String statemgrClass = Context.stateManagerClass(config);
    IStateManager statemgr = ReflectionUtils.newInstance(statemgrClass);
    statemgr.initialize(config);
    SchedulerStateManagerAdaptor adaptor = new SchedulerStateManagerAdaptor(statemgr, 5000);
    // build the runtime config
    LauncherUtils launcherUtils = LauncherUtils.getInstance();
    this.runtime = Config.newBuilder()
        .putAll(launcherUtils.getPrimaryRuntime(topology, adaptor))
        .put(Keys.schedulerShutdown(), getShutdown())
        .put(Keys.SCHEDULER_PROPERTIES, schedulerProperties)
        .build();
  }

  private ScheduledFuture<?> start() {
    executor = Executors.newScheduledThreadPool(1);
    ScheduledFuture<?> future = executor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        LOG.info("Executing SLA Policy: " + policy.getClass().getSimpleName());
        policy.execute();
      }
    }, 1, 15, TimeUnit.SECONDS);

    return future;
  }

  // Utils method
  protected Shutdown getShutdown() {
    return new Shutdown();
  }

}
