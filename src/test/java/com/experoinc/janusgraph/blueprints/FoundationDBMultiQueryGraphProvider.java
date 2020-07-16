package com.experoinc.janusgraph.blueprints;

import com.experoinc.janusgraph.FoundationDBContainer;
import org.janusgraph.blueprints.AbstractJanusGraphProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.ClassRule;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBMultiQueryGraphProvider extends AbstractJanusGraphProvider {

    @ClassRule
    public static FoundationDBContainer container = new FoundationDBContainer();

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return container.getFoundationDBConfiguration()
            .set(USE_MULTIQUERY, true);
    }
}
