package org.neo4j.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.DocumentResult;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.elasticsearch.ElasticSearchHandler.IndexId;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;

/**
 * @author mh
 * @since 25.04.15
 */
class ElasticSearchEventListener implements
    TransactionEventListener<Collection<BulkableAction<DocumentResult>>> {

    private final static Logger logger = Logger.getLogger(
        ElasticSearchEventListener.class.getName());

    private final ElasticSearchHandler handler;

    public ElasticSearchEventListener(ElasticSearchHandler handler) {
        this.handler = handler;
    }

    @Override
    public Collection<BulkableAction<DocumentResult>> beforeCommit(
        TransactionData data,
        Transaction transaction,
        GraphDatabaseService databaseService
    ) throws Exception {
        Map<IndexId, BulkableAction<DocumentResult>> actions = new HashMap<>(1000);

        for (Node node : data.createdNodes()) {
            if (handler.hasLabel(node)) {
                actions.putAll(handler.indexRequests(node));
            }
        }
        for (LabelEntry labelEntry : data.assignedLabels()) {
            if (handler.hasLabel(labelEntry)) {
                if (data.isDeleted(labelEntry.node())) {
                    actions.putAll(handler.deleteRequests(labelEntry.node()));
                } else {
                    actions.putAll(handler.indexRequests(labelEntry.node()));
                }
            }
        }
        for (LabelEntry labelEntry : data.removedLabels()) {
            if (handler.hasLabel(labelEntry)) {
                actions.putAll(handler.deleteRequests(labelEntry.node(), labelEntry.label()));
            }
        }
        for (PropertyEntry<Node> propEntry : data.assignedNodeProperties()) {
            if (handler.hasLabel(propEntry)) {
                actions.putAll(handler.indexRequests(propEntry.entity()));
            }
        }
        for (PropertyEntry<Node> propEntry : data.removedNodeProperties()) {
            if (!data.isDeleted(propEntry.entity()) && handler.hasLabel(propEntry)) {
                actions.putAll(handler.updateRequests(propEntry.entity()));
            }
        }
        return actions.isEmpty() ? Collections.emptyList() : actions.values();
    }

    @Override
    public void afterCommit(
        TransactionData data,
        Collection<BulkableAction<DocumentResult>> actions,
        GraphDatabaseService databaseService
    ) {
        if (actions.isEmpty()) {
            return;
        }
        try {
            handler.execute(actions);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error updating ElasticSearch ", e);
        }
    }

    @Override
    public void afterRollback(
        TransactionData data,
        Collection<BulkableAction<DocumentResult>> actions,
        GraphDatabaseService databaseService
    ) {

    }

}
