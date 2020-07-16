package com.experoinc.janusgraph;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.CLUSTER_FILE_PATH;
import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;
import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
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

public class FoundationDBContainer extends FixedHostPortGenericContainer<FoundationDBContainer> {

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
      System.err.println("Couldn't open random socket, using default!");
      return DEFAULT_PORT;
    }
  }

  @Override
  public void start() {
    super.start();
    // initialize the database
    Container.ExecResult lsResult;
    try {
      lsResult = this.execInContainer("fdbcli", "--exec", "configure new single ssd");
    } catch (UnsupportedOperationException | IOException | InterruptedException e) {
      throw new ContainerLaunchException("Container startup failed. Failed to initialized the database.");
    }
    if(lsResult.getExitCode() != 0) {
      throw new ContainerLaunchException("Container startup failed. Failed to initialized the database.");
    }
  }
  
  public ModifiableConfiguration getFoundationDBConfiguration() {
    return getFoundationDBConfiguration("janusgraph-test-fdb");
  }

  public ModifiableConfiguration getFoundationDBConfiguration(final String graphName) {
    return buildGraphConfiguration()
      .set(STORAGE_BACKEND,"com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager")
      .set(DIRECTORY, graphName)
      .set(DROP_ON_CLEAR, false)
      .set(CLUSTER_FILE_PATH, "target/test-classes/fdb/fdb.cluster")
      .set(ISOLATION_LEVEL, "read_committed_with_write");
  }

  public WriteConfiguration getFoundationDBGraphConfiguration() {
    return getFoundationDBConfiguration().getConfiguration();
  }

}