/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.spark;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.Starter;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;
import org.apache.seatunnel.core.starter.utils.CompressionUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A Starter to generate spark-submit command for SeaTunnel job on spark. */
public class SparkStarter implements Starter {

    /** original commandline args */
    protected String[] args;

    /** args parsed from {@link #args} */
    protected SparkCommandArgs commandArgs;

    /** jars to include on the spark driver and executor classpaths */
    protected List<Path> jars = new ArrayList<>();

    /** files to be placed in the working directory of each spark executor */
    protected List<Path> files = new ArrayList<>();

    /** spark configuration properties */
    protected Map<String, String> sparkConf;

    private SparkStarter(String[] args, SparkCommandArgs commandArgs) {
        this.args = args;
        this.commandArgs = commandArgs;
    }

    public static void main(String[] args) throws IOException {
        SparkStarter starter = getInstance(args);
        List<String> command = starter.buildCommands();
        System.out.println(String.join(" ", command));
    }

    /**
     * method to get SparkStarter instance, will return {@link ClusterModeSparkStarter} or {@link
     * ClientModeSparkStarter} depending on deploy mode.
     */
    static SparkStarter getInstance(String[] args) {
        SparkCommandArgs commandArgs =
                CommandLineUtils.parse(
                        args,
                        new SparkCommandArgs(),
                        EngineType.SPARK3.getStarterShellName(),
                        true);
        DeployMode deployMode = commandArgs.getDeployMode();
        switch (deployMode) {
            case CLUSTER:
                return new ClusterModeSparkStarter(args, commandArgs);
            case CLIENT:
                return new ClientModeSparkStarter(args, commandArgs);
            default:
                throw new IllegalArgumentException("Deploy mode " + deployMode + " not supported");
        }
    }

    @Override
    public List<String> buildCommands() throws IOException {
        setSparkConf();
        Common.setDeployMode(commandArgs.getDeployMode());
        Common.setStarter(true);
        this.jars.addAll(Common.getLibJars());
        this.jars.addAll(getConnectorJarDependencies());
        this.jars.addAll(
                new ArrayList<>(
                        Common.getThirdPartyJars(
                                sparkConf.getOrDefault(EnvCommonOptions.JARS.key(), ""))));
        // TODO: override job name in command args, because in spark cluster deploy mode
        // command-line arguments are read first
        // if user has not specified job with command line, the job name config in file will not
        // work
        return buildFinal();
    }

    /** parse spark configurations from SeaTunnel config file */
    private void setSparkConf() throws FileNotFoundException {
        this.sparkConf = getSparkConf(commandArgs.getConfigFile(), commandArgs.getVariables());
    }

    /** Get spark configurations from SeaTunnel job config file. */
    static Map<String, String> getSparkConf(String configFile, List<String> variables) {
        Config appConfig = ConfigBuilder.of(configFile, variables);
        return appConfig.getConfig("env").entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey, e -> e.getValue().unwrapped().toString()));
    }

    /** return connector's jars, which located in 'connectors/*'. */
    private List<Path> getConnectorJarDependencies() {
        Path pluginRootDir = Common.connectorDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        Config config = ConfigBuilder.of(commandArgs.getConfigFile(), commandArgs.getVariables());
        Set<URL> pluginJars = new HashSet<>();
        SeaTunnelSourcePluginDiscovery seaTunnelSourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery();
        SeaTunnelSinkPluginDiscovery seaTunnelSinkPluginDiscovery =
                new SeaTunnelSinkPluginDiscovery();
        pluginJars.addAll(
                seaTunnelSourcePluginDiscovery.getPluginJarAndDependencyPaths(
                        getPluginIdentifiers(config, PluginType.SOURCE)));
        if (config.hasPath(PluginType.TRANSFORM.getType())) {
            pluginJars.addAll(
                    seaTunnelSinkPluginDiscovery.getPluginJarAndDependencyPaths(
                            getPluginIdentifiers(config, PluginType.TRANSFORM)));
        }
        pluginJars.addAll(
                seaTunnelSinkPluginDiscovery.getPluginJarAndDependencyPaths(
                        getPluginIdentifiers(config, PluginType.SINK)));
        return pluginJars.stream()
                .map(url -> new File(url.getPath()).toPath())
                .distinct()
                .collect(Collectors.toList());
    }

    /** build final spark-submit commands */
    protected List<String> buildFinal() {
        List<String> commands = new ArrayList<>();
        commands.add("${SPARK_HOME}/bin/spark-submit");
        appendOption(commands, "--class", SeaTunnelSpark.class.getName());
        appendOption(commands, "--name", this.commandArgs.getJobName());
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getDeployMode());
        appendJars(commands, this.jars);
        appendFiles(commands, this.files);
        appendSparkConf(commands, this.sparkConf);
        appendAppJar(commands);
        appendOption(commands, "--config", this.commandArgs.getConfigFile());
        appendOption(commands, "--master", this.commandArgs.getMaster());
        appendOption(commands, "--deploy-mode", this.commandArgs.getDeployMode().getDeployMode());
        appendOption(commands, "--name", this.commandArgs.getJobName());
        if (commandArgs.isEncrypt()) {
            commands.add("--encrypt");
        }
        if (commandArgs.isDecrypt()) {
            commands.add("--decrypt");
        }
        if (this.commandArgs.isCheckConfig()) {
            commands.add("--check");
        }
        this.commandArgs.getVariables().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .forEach(variable -> commands.add("-i " + variable));
        return commands;
    }

    /** append option to StringBuilder */
    protected void appendOption(List<String> commands, String option, String value) {
        commands.add(option);
        commands.add("\"" + value.replace("\"", "\\\"") + "\"");
    }

    /** append jars option to StringBuilder */
    protected void appendJars(List<String> commands, List<Path> paths) {
        appendPaths(commands, "--jars", paths);
    }

    /** append files option to StringBuilder */
    protected void appendFiles(List<String> commands, List<Path> paths) {
        appendPaths(commands, "--files", paths);
    }

    /** append comma-split paths option to StringBuilder */
    protected void appendPaths(List<String> commands, String option, List<Path> paths) {
        if (!paths.isEmpty()) {
            String values = paths.stream().map(Path::toString).collect(Collectors.joining(","));
            appendOption(commands, option, values);
        }
    }

    /** append spark configurations to StringBuilder */
    protected void appendSparkConf(List<String> commands, Map<String, String> sparkConf) {
        for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            appendOption(commands, "--conf", key + "=" + value);
        }
    }

    /**
     * Append the starter jar path to the spark-submit command.
     *
     * <p>Historically this hard-coded {@code EngineType.SPARK3.getStarterJarName()}, which broke
     * every module whose physical artifact wasn't literally {@code seatunnel-spark-3-starter.jar} —
     * for example {@code seatunnel-spark-3.5-starter.jar} and {@code
     * seatunnel-spark-4.1-starter.jar}, both of which reuse this class from {@code
     * seatunnel-spark-starter-common}. Spark-submit would then fail with {@code JAR file does not
     * exist}.
     *
     * <p>Now we detect the loading location of {@link SparkStarter} at runtime; whichever
     * spark-N-starter.jar pulled this class in wins. Falls back to the SPARK3 enum default only if
     * the code source can't be resolved (bare-classpath / IDE runs).
     */
    protected void appendAppJar(List<String> commands) {
        commands.add(resolveAppJarPath());
    }

    static String resolveAppJarPath() {
        try {
            java.net.URL src =
                    SparkStarter.class.getProtectionDomain().getCodeSource().getLocation();
            if (src != null) {
                String path = new File(src.toURI()).getAbsolutePath();
                if (path.endsWith(".jar")) {
                    return path;
                }
            }
        } catch (Exception ignored) {
            // fall through to enum default
        }
        return Common.appStarterDir().resolve(EngineType.SPARK3.getStarterJarName()).toString();
    }

    private List<PluginIdentifier> getPluginIdentifiers(Config config, PluginType... pluginTypes) {
        return Arrays.stream(pluginTypes)
                .flatMap(
                        (Function<PluginType, Stream<PluginIdentifier>>)
                                pluginType -> {
                                    List<? extends Config> configList =
                                            config.getConfigList(pluginType.getType());
                                    return configList.stream()
                                            .map(
                                                    pluginConfig ->
                                                            PluginIdentifier.of(
                                                                    "seatunnel",
                                                                    pluginType.getType(),
                                                                    pluginConfig.getString(
                                                                            "plugin_name")));
                                })
                .collect(Collectors.toList());
    }

    /** a Starter for building spark-submit commands with client mode options */
    private static class ClientModeSparkStarter extends SparkStarter {

        /** client mode specified spark options */
        private enum ClientModeSparkConfigs {

            /** Memory for driver in client mode */
            DriverMemory("--driver-memory", "spark.driver.memory"),

            /** Extra Java options to pass to the driver in client mode */
            DriverJavaOptions("--driver-java-options", "spark.driver.extraJavaOptions"),

            /** Extra library path entries to pass to the driver in client mode */
            DriverLibraryPath(" --driver-library-path", "spark.driver.extraLibraryPath"),

            /** Extra class path entries to pass to the driver in client mode */
            DriverClassPath("--driver-class-path", "spark.driver.extraClassPath");

            private final String optionName;

            private final String propertyName;

            private static final Map<String, ClientModeSparkConfigs> PROPERTY_NAME_MAP =
                    new HashMap<>();

            static {
                for (ClientModeSparkConfigs config : values()) {
                    PROPERTY_NAME_MAP.put(config.propertyName, config);
                }
            }

            ClientModeSparkConfigs(String optionName, String propertyName) {
                this.optionName = optionName;
                this.propertyName = propertyName;
            }
        }

        private ClientModeSparkStarter(String[] args, SparkCommandArgs commandArgs) {
            super(args, commandArgs);
        }

        @Override
        protected void appendSparkConf(List<String> commands, Map<String, String> sparkConf) {
            for (ClientModeSparkConfigs config : ClientModeSparkConfigs.values()) {
                String driverJavaOptions = this.sparkConf.get(config.propertyName);
                if (StringUtils.isNotBlank(driverJavaOptions)) {
                    appendOption(commands, config.optionName, driverJavaOptions);
                }
            }
            for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (ClientModeSparkConfigs.PROPERTY_NAME_MAP.containsKey(key)) {
                    continue;
                }
                appendOption(commands, "--conf", key + "=" + value);
            }
        }
    }

    /** a Starter for building spark-submit commands with cluster mode options */
    private static class ClusterModeSparkStarter extends SparkStarter {

        private ClusterModeSparkStarter(String[] args, SparkCommandArgs commandArgs) {
            super(args, commandArgs);
        }

        @Override
        public List<String> buildCommands() throws IOException {
            Common.setDeployMode(commandArgs.getDeployMode());
            Common.setStarter(true);
            Path pluginTarball = resolvePluginTarballPath();
            CompressionUtils.tarGzip(Common.pluginRootDir(), pluginTarball);
            this.files.add(pluginTarball);
            this.files.add(Paths.get(commandArgs.getConfigFile()));
            return super.buildCommands();
        }

        /**
         * Resolve a user-writable path for the plugins tarball that spark-submit ships via
         * {@code --files}.
         *
         * <p>{@link Common#pluginTarball()} resolves to {@code ${SEATUNNEL_HOME}/plugins.tar.gz},
         * but on packaged installs (RPM/mpack) {@code $SEATUNNEL_HOME} is owned by root while the
         * submitting user is typically a service account (seatunnel, spark, ...). Writing a
         * submit-time artefact into the root-owned install root fails with {@link
         * java.nio.file.AccessDeniedException} on the first submission under each new user.
         *
         * <p>The tarball is throwaway — spark-submit reads it synchronously before shipping to
         * YARN, and the file isn't referenced again by this process. Put it under {@code
         * java.io.tmpdir} with a per-invocation suffix (process PID + millis) to avoid colliding
         * with concurrent submissions from the same user, and register for {@code deleteOnExit}
         * so stale copies don't accumulate.
         */
        private static Path resolvePluginTarballPath() {
            String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            Path tarball =
                    Paths.get(
                            tmpDir,
                            "seatunnel-plugins-" + pid + "-" + System.currentTimeMillis()
                                    + ".tar.gz");
            tarball.toFile().deleteOnExit();
            return tarball;
        }
    }
}
