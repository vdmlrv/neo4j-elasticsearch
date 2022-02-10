package org.neo4j.elasticsearch;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.elasticsearch.ElasticSearchKernelExtensionFactory.Dependencies;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * @author mh
 * @since 06.02.13
 */
public class ElasticSearchKernelExtensionFactory extends ExtensionFactory<Dependencies> {

    public static final String SERVICE_NAME = "ELASTIC_SEARCH";

    public ElasticSearchKernelExtensionFactory() {
        super(SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        Config config = dependencies.getConfig();

        return new ElasticSearchExtension(dependencies.getDatabaseManagementService(),
            config.get(ElasticSearchSettings.hostName),
            config.get(ElasticSearchSettings.indexSpec),
            config.get(ElasticSearchSettings.discovery),
            config.get(ElasticSearchSettings.includeIDField),
            config.get(ElasticSearchSettings.includeLabelsField));
    }

    public interface Dependencies {

        DatabaseManagementService getDatabaseManagementService();

        Config getConfig();
    }
}
