package com.experoinc.janusgraph.blueprints;

import com.experoinc.janusgraph.FoundationDBContainer;
import org.janusgraph.blueprints.AbstractJanusGraphProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USE_MULTIQUERY;

/**
 * @author Ted Wilmes (twilmes@gmail.com)
 */
public class FoundationDBMultiQueryGraphProvider extends AbstractJanusGraphProvider {

    public static final FoundationDBContainer fdbContainer = new FoundationDBContainer();

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        fdbContainer.start();
        if(graphName != null) {
            return fdbContainer.getFoundationDBConfiguration(graphName).set(USE_MULTIQUERY, true);
        }
        return fdbContainer.getFoundationDBConfiguration().set(USE_MULTIQUERY, true);
    }
}
