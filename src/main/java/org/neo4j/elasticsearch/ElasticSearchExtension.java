package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import java.text.ParseException;
import java.util.List;
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
    private boolean enableAutoIndex = true;
    private final boolean discovery;
    private ElasticSearchHandler handler;
    private ElasticSearchEventListener listener;
    private JestClient client;
    private ElasticSearchIndexSettings indexSettings;

    public ElasticSearchExtension(
        DatabaseManagementService dms,
        String hostName,
        String indexSpec,
        Boolean discovery,
        Boolean includeIDField,
        Boolean includeLabelsField,
        Boolean enabledAutoIndex
    ) {
        try {
            Map<String, List<ElasticSearchIndexSpec>> iSpec = ElasticSearchIndexSpecParser.parseIndexSpec(
                indexSpec);
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
        this.enableAutoIndex = enabledAutoIndex;
    }

    @Override
    public void init() throws Exception {
        if (!enabled) {
            return;
        }

        client = getJestClient(hostName, discovery);
        handler = ElasticSearchHandler.newInstance(client, indexSettings);
        if (enableAutoIndex) {
            listener = new ElasticSearchEventListener(handler);
            dms.registerTransactionEventListener("neo4j", listener);
        }
        logger.info("Connecting to ElasticSearch");
    }

    @Override
    public void shutdown() throws Exception {
        if (!enabled) {
            return;
        }
        if (enableAutoIndex) {
            dms.unregisterTransactionEventListener("neo4j", listener);
        }
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
