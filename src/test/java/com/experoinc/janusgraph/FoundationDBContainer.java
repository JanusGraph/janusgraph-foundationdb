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
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBContainer extends FixedHostPortGenericContainer<FoundationDBContainer> {

    public FoundationDBContainer() {
        super("twilmes/foundationdb:5.2.5-ubuntu-18.04");
        withExposedPorts(4500);
        withFixedExposedPort(4500, 4500);
        withFileSystemBind("./etc", "/etc/foundationdb");
        waitingFor(
                Wait.forLogMessage(".* FDBD joined cluster.*\\n", 1)
        );
    }
    public ModifiableConfiguration getFoundationDBConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb");
    }

    public ModifiableConfiguration getFoundationDBConfiguration(final String graphName) {
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND,"com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager")
                .set(DIRECTORY, graphName)
                .set(DROP_ON_CLEAR, false)
                .set(CLUSTER_FILE_PATH, "etc/fdb.cluster")
                .set(ISOLATION_LEVEL, "read_committed_with_write");
    }

    public WriteConfiguration getFoundationDBGraphConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb").getConfiguration();
    }
}
