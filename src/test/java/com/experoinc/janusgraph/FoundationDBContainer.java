// Copyright 2018 Expero Inc.
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

package com.experoinc.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.junit.Assert;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBContainer extends FixedHostPortGenericContainer<FoundationDBContainer> {
    private boolean firstTime = true;

    public FoundationDBContainer() {
        super("foundationdb/foundationdb:6.1.12");
        withExposedPorts(4500);
        withFixedExposedPort(4500, 4500);
        withFileSystemBind("./etc", "/etc/foundationdb");
        withEnv("FDB_NETWORKING_MODE", "host");
        waitingFor(
                Wait.forLogMessage(".*FDBD joined cluster.*\\n", 1)
        );

    }

    public ModifiableConfiguration getFoundationDBConfiguration() {
        if (firstTime) {
            try {
                ExecResult fdbcli = execInContainer("fdbcli", "--exec", "configure new single ssd");
                int exitCode = fdbcli.getExitCode();
                if(exitCode != 0)
                    throw new RuntimeException("test");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            firstTime = false;
        }
        return getFoundationDBConfiguration("janusgraph-test-fdb");
    }

    public ModifiableConfiguration getFoundationDBConfiguration(final String graphName) {
        String s = "docker:docker@127.0.0.1:4500";// + getMappedPort(4500);
        File clusterFile = null;
        try {
            clusterFile = File.createTempFile("fdb", "cluster");

            FileWriter fr = new FileWriter(clusterFile);
            fr.write(s);
            fr.close();
        } catch (IOException e) {
            Assert.fail();
        }
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND, "com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager")
                .set(DIRECTORY, graphName)
                .set(DROP_ON_CLEAR, false)
                .set(CLUSTER_FILE_PATH, clusterFile.getAbsolutePath())
                .set(ISOLATION_LEVEL, "read_committed_with_write");
    }

    public WriteConfiguration getFoundationDBGraphConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb").getConfiguration();
    }
}
