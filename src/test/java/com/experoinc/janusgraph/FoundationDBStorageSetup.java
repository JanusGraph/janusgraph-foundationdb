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

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.CLUSTER_FILE_PATH;
import static com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBStorageSetup extends StorageSetup {


    public static ModifiableConfiguration getFoundationDBConfiguration() {
        return getFoundationDBConfiguration("janusgraph-test-fdb");
    }

    public static ModifiableConfiguration getFoundationDBConfiguration(final String graphName) {
        return buildGraphConfiguration()
                .set(STORAGE_BACKEND,"com.experoinc.janusgraph.diskstorage.foundationdb.FoundationDBStoreManager")
                .set(DIRECTORY, graphName)
                .set(DROP_ON_CLEAR, false)
                .set(CLUSTER_FILE_PATH, "src/test/resources/etc/fdb.cluster")
                .set(ISOLATION_LEVEL, "read_committed_with_write");
    }

    public static WriteConfiguration getFoundationDBGraphConfiguration() {
        return getFoundationDBConfiguration().getConfiguration();
    }

    public static DockerComposeRule startFoundationDBDocker() {
        return DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForService("db", HealthChecks.toHaveAllPortsOpen())
            .build();
    }
}
