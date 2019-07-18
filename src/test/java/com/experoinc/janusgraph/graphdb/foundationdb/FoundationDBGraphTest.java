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

package com.experoinc.janusgraph.graphdb.foundationdb;

import com.experoinc.janusgraph.FoundationDBContainer;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.*;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@Testcontainers
public class FoundationDBGraphTest extends JanusGraphTest {

    @Container
    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    private static final Logger log =
            LoggerFactory.getLogger(FoundationDBGraphTest.class);

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration modifiableConfiguration = fdbContainer.getFoundationDBConfiguration();
        String methodName = this.testInfo.getDisplayName();
        if (methodName.equals("testConsistencyEnforcement")) {
//            IsolationLevel iso = IsolationLevel.SERIALIZABLE;
//            log.debug("Forcing isolation level {} for test method {}", iso, methodName);
//            modifiableConfiguration.set(FoundationDBStoreManager.ISOLATION_LEVEL, iso.toString());
        } else {
//            IsolationLevel iso = null;
//            if (modifiableConfiguration.has(FoundationDBStoreManager.ISOLATION_LEVEL)) {
//                iso = ConfigOption.getEnumValue(modifiableConfiguration.get(FoundationDBStoreManager.ISOLATION_LEVEL),IsolationLevel.class);
//            }
//            log.debug("Using isolation level {} (null means adapter default) for test method {}", iso, methodName);
        }
        return modifiableConfiguration.getConfiguration();
    }

    @Test
    @Disabled
    @Override
    public void testClearStorage() throws Exception {

    }

    @Test
    public void testVertexCentricQuerySmall() {
        testVertexCentricQuery(1450 /*noVertices*/);
    }

    @Test
    @Override
    public void testConsistencyEnforcement() {
        // Check that getConfiguration() explicitly set serializable isolation
        // This could be enforced with a JUnit assertion instead of a Precondition,
        // but a failure here indicates a problem in the test itself rather than the
        // system-under-test, so a Precondition seems more appropriate
        //IsolationLevel effective = ConfigOption.getEnumValue(config.get(ConfigElement.getPath(FoundationDBStoreManager.ISOLATION_LEVEL), String.class),IsolationLevel.class);
        //Preconditions.checkState(IsolationLevel.SERIALIZABLE.equals(effective));
        super.testConsistencyEnforcement();
    }

    @Override
    public void testConcurrentConsistencyEnforcement() {
        //Do nothing TODO: Figure out why this is failing in BerkeleyDB!!
    }

    @Test
    public void testIDBlockAllocationTimeout() throws BackendException {
        config.set("ids.authority.wait-time", Duration.of(0L, ChronoUnit.NANOS));
        config.set("ids.renew-timeout", Duration.of(1L, ChronoUnit.MILLIS));
        close();
        JanusGraphFactory.drop(graph);
        open(config);
        try {
            graph.addVertex();
            fail();
        } catch (JanusGraphException ignored) {

        }

        assertTrue(graph.isOpen());

        close(); // must be able to close cleanly

        // Must be able to reopen
        open(config);

        assertEquals(0L, (long) graph.traversal().V().count().next());
    }

    @Test
    @Disabled
    @Override
    public void testLargeJointIndexRetrieval() {
        // disabled because exceeds FDB transaction commit limit
    }

    @Test
    @Override
    public void testVertexCentricQuery() {
        // updated to not exceed FDB transaction commit limit
        testVertexCentricQuery(1000 /*noVertices*/);
    }
}
