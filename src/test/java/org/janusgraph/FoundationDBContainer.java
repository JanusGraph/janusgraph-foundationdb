// Copyright 2020 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph;

import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.CLUSTER_FILE_PATH;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.GET_RANGE_MODE;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.DROP_ON_CLEAR;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

import java.io.IOException;
import java.net.ServerSocket;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FoundationDBContainer extends FixedHostPortGenericContainer<FoundationDBContainer> {

    private final Logger log = LoggerFactory.getLogger(FoundationDBContainer.class);

    public static final String DEFAULT_IMAGE_AND_TAG = "foundationdb/foundationdb:6.2.20";
    private static final Integer DEFAULT_PORT = 4500;
    private static final String FDB_CLUSTER_FILE_ENV_KEY = "FDB_CLUSTER_FILE";
    private static final String FDB_NETWORKING_MODE_ENV_KEY = "FDB_NETWORKING_MODE";
    private static final String FDB_PORT_ENV_KEY = "FDB_PORT";
    private static final String DEFAULT_NETWORKING_MODE = "host";
    private static final String DEFAULT_CLUSTER_FILE_PARENT_DIR = "/etc/foundationdb";
    private static final String DEFAULT_CLUSTER_FILE_PATH = DEFAULT_CLUSTER_FILE_PARENT_DIR + "/" + "fdb.cluster";
    private static final String DEFAULT_VOLUME_SOURCE_PATH = "./fdb";


    public FoundationDBContainer(String dockerImageName) {
        super(dockerImageName);
        Integer port = findRandomOpenPortOnAllLocalInterfaces();
        this.addFixedExposedPort(port, port);
        this.addExposedPorts(port);
        this.addEnv(FDB_CLUSTER_FILE_ENV_KEY, DEFAULT_CLUSTER_FILE_PATH);
        this.addEnv(FDB_PORT_ENV_KEY, port.toString());
        this.addEnv(FDB_NETWORKING_MODE_ENV_KEY, DEFAULT_NETWORKING_MODE);
        this.withClasspathResourceMapping(DEFAULT_VOLUME_SOURCE_PATH, DEFAULT_CLUSTER_FILE_PARENT_DIR, BindMode.READ_WRITE);
    }

    public FoundationDBContainer(){
        this(DEFAULT_IMAGE_AND_TAG);
    }

    private Integer findRandomOpenPortOnAllLocalInterfaces() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (Exception e) {
            log.error("Couldn't open random port, using default port '%d'.", DEFAULT_PORT);
            return DEFAULT_PORT;
        }
    }

    @Override
    public void start() {
        if (this.getContainerId() != null) {
            // Already started this container.
            return;
        }
        super.start();
        // initialize the database
        Container.ExecResult execResult;
        try {
            execResult = this.execInContainer("fdbcli", "--exec", "configure new single ssd");
        } catch (UnsupportedOperationException | IOException | InterruptedException e) {
            throw new ContainerLaunchException("Container startup failed. Failed to initialize the database.", e);
        }
        if (execResult.getExitCode() != 0) {
            throw new ContainerLaunchException("Container startup failed. Failed to initialize the database. Received non zero exit code from fdbcli command. Response code was: " + execResult.getExitCode() + ".");
        }
    }

    public ModifiableConfiguration getFoundationDBConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb");
    }

    private String getAndCheckRangeModeFromTestEnvironment() {
        String mode = System.getProperty("getrangemode");
        if (mode == null) {
            log.warn("No getrangemode property is chosen, use default value: list to proceed");
            return "list";
        }
        else if (mode.equalsIgnoreCase("iterator")){
            log.info("getrangemode property is chosen as: iterator");
            return "iterator";
        }
        else if (mode.equalsIgnoreCase("list")){
            log.info("getrangemode property is chosen as: list");
            return "list";
        }
        else {
            log.warn("getrange mode property chosen: " +  mode + " does not match supported modes: iterator or list, choose default value: list to proceed");
            return "list";
        }
    }

    public ModifiableConfiguration getFoundationDBConfiguration(final String graphName) {
        ModifiableConfiguration config = buildGraphConfiguration()
            .set(STORAGE_BACKEND,"org.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager")
            .set(DIRECTORY, graphName)
            .set(DROP_ON_CLEAR, false)
            .set(CLUSTER_FILE_PATH, "target/test-classes/fdb/fdb.cluster")
            .set(ISOLATION_LEVEL, "read_committed_with_write")
            .set(GET_RANGE_MODE, getAndCheckRangeModeFromTestEnvironment())
            .set(VERSION, 620);

        return config;
    }

    public WriteConfiguration getFoundationDBGraphConfiguration() {
        return getFoundationDBConfiguration().getConfiguration();
    }

}
