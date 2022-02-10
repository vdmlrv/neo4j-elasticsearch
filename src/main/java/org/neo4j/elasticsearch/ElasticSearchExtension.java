package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import java.text.ParseException;
import java.util.Map;
import java.util.logging.Logger;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * @author mh
 * @since 25.04.15
 */
public class ElasticSearchExtension extends LifecycleAdapter {

    private final DatabaseManagementService dms;
    private final static Logger logger = Logger.getLogger(ElasticSearchExtension.class.getName());
    private final String hostName;
    private boolean enabled = true;
    private final boolean discovery;
    private ElasticSearchEventHandler handler;
    private JestClient client;
    private ElasticSearchIndexSettings indexSettings;

    public ElasticSearchExtension(DatabaseManagementService dms, String hostName, String indexSpec,
        Boolean discovery, Boolean includeIDField, Boolean includeLabelsField) {
        Map iSpec;
        try {
            iSpec = ElasticSearchIndexSpecParser.parseIndexSpec(indexSpec);
            if (iSpec.size() == 0) {
                logger.severe("ElasticSearch Integration: syntax error in index_spec");
                enabled = false;
            }
            this.indexSettings = new ElasticSearchIndexSettings(iSpec, includeIDField,
                includeLabelsField);
        } catch (ParseException e) {
            logger.severe("ElasticSearch Integration: Can't define index twice");
            enabled = false;
        }
        logger.info("Elasticsearch Integration: Running " + hostName + " - " + indexSpec);
        this.dms = dms;
        this.hostName = hostName;
        this.discovery = discovery;
    }

    @Override
    public void init() throws Exception {
        if (!enabled) {
            return;
        }

        client = getJestClient(hostName, discovery);
        handler = new ElasticSearchEventHandler(client, indexSettings);
        dms.registerTransactionEventListener("neo4j", handler);
        logger.info("Connecting to ElasticSearch");
    }

    @Override
    public void shutdown() throws Exception {
        if (!enabled) {
            return;
        }
        dms.unregisterTransactionEventListener("neo4j", handler);
        client.close();
        logger.info("Disconnected from ElasticSearch");
    }

    private JestClient getJestClient(final String hostName, final Boolean discovery)
        throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(JestDefaultHttpConfigFactory.getConfigFor(hostName, discovery));
        return factory.getObject();
    }
}
