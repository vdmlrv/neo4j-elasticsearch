package org.neo4j.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Update;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;

public class ElasticSearchHandler implements JestResultHandler<JestResult> {

    private final static Logger logger = Logger.getLogger(
        ElasticSearchHandler.class.getName());

    private static ElasticSearchHandler instance;

    public static ElasticSearchHandler getInstance() {
        return instance;
    }

    public static ElasticSearchHandler newInstance(
        JestClient jestClient,
        ElasticSearchIndexSettings indexSettings) {
        instance = new ElasticSearchHandler(jestClient, indexSettings);
        return instance;
    }

    private final JestClient jestClient;
    private final ElasticSearchIndexSettings indexSettings;
    private final Set<String> indexLabels;

    private boolean useAsyncJest = true;

    private ElasticSearchHandler(
        JestClient jestClient,
        ElasticSearchIndexSettings indexSettings
    ) {
        this.jestClient = jestClient;
        this.indexSettings = indexSettings;
        this.indexLabels = indexSettings.getIndexSpec().keySet();
    }

    public void index(Node node) throws IOException {
        if (hasLabel(node)) {
            Map<IndexId, BulkableAction<DocumentResult>> actions =
                new HashMap<>(indexRequests(node));
            execute(actions.values(), false);
        }
    }

    public Map<IndexId, Index> indexRequests(Node node) {
        HashMap<IndexId, Index> reqs = new HashMap<>();

        for (Label l : node.getLabels()) {
            if (!indexLabels.contains(l.name())) {
                continue;
            }

            for (ElasticSearchIndexSpec spec : indexSettings.getIndexSpec().get(l.name())) {
                String id = id(node), indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id),
                    new Index.Builder(nodeToJson(node, spec.getProperties()))
                        .type(l.name())
                        .index(indexName)
                        .id(id)
                        .build());
            }
        }
        return reqs;
    }

    public Map<IndexId, Delete> deleteRequests(Node node) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

        for (Label l : node.getLabels()) {
            if (!indexLabels.contains(l.name())) {
                continue;
            }
            for (ElasticSearchIndexSpec spec : indexSettings.getIndexSpec().get(l.name())) {
                String id = id(node), indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id),
                    new Delete.Builder(id).index(indexName).build());
            }
        }
        return reqs;
    }

    public Map<IndexId, Delete> deleteRequests(Node node, Label label) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

        if (indexLabels.contains(label.name())) {
            for (ElasticSearchIndexSpec spec : indexSettings.getIndexSpec().get(label.name())) {
                String id = id(node), indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id),
                    new Delete.Builder(id)
                        .index(indexName)
                        .type(label.name())
                        .build());
            }
        }
        return reqs;
    }

    public Map<IndexId, Update> updateRequests(Node node) {
        HashMap<IndexId, Update> reqs = new HashMap<>();
        for (Label l : node.getLabels()) {
            if (!indexLabels.contains(l.name())) {
                continue;
            }

            for (ElasticSearchIndexSpec spec : indexSettings.getIndexSpec().get(l.name())) {
                String id = id(node), indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id),
                    new Update.Builder(nodeToJson(node, spec.getProperties()))
                        .type(l.name())
                        .index(spec.getIndexName())
                        .id(id(node))
                        .build());
            }
        }
        return reqs;
    }

    public boolean hasLabel(Node node) {
        for (Label l : node.getLabels()) {
            if (indexLabels.contains(l.name())) {
                return true;
            }
        }
        return false;
    }

    public boolean hasLabel(LabelEntry labelEntry) {
        return indexLabels.contains(labelEntry.label().name());
    }

    public boolean hasLabel(PropertyEntry<Node> propEntry) {
        return hasLabel(propEntry.entity());
    }

    private String id(Node node) {
        return String.valueOf(node.getId());
    }

    private List<String> labels(Node node) {
        List<String> result = new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result;
    }

    private Map<String, Object> nodeToJson(Node node, Set<String> properties) {
        Map<String, Object> json = new LinkedHashMap<>();

        if (indexSettings.getIncludeIDField()) {
            json.put("id", id(node));
        }

        if (indexSettings.getIncludeLabelsField()) {
            json.put("labels", labels(node));
        }

        for (String prop : properties) {
            if (node.hasProperty(prop)) {
                Object value = node.getProperty(prop);
                json.put(prop, value);
            }
        }
        return json;
    }

    public void setUseAsyncJest(boolean useAsyncJest) {
        this.useAsyncJest = useAsyncJest;
    }

    public void execute(Collection<BulkableAction<DocumentResult>> actions) throws IOException {
        execute(actions, useAsyncJest);
    }

    public void execute(Collection<BulkableAction<DocumentResult>> actions, boolean useAsyncJest)
        throws IOException {
        Bulk bulk = new Bulk.Builder()
            .addAction(actions).build();
        if (useAsyncJest) {
            jestClient.executeAsync(bulk, this);
        } else {
            jestClient.execute(bulk);
        }
    }

    @Override
    public void completed(JestResult jestResult) {
        if (jestResult.isSucceeded() && jestResult.getErrorMessage() == null) {
            logger.fine("ElasticSearch Update Success");
        } else {
            logger.severe("ElasticSearch Update Failed: " + jestResult.getErrorMessage());
        }
    }

    @Override
    public void failed(Exception e) {
        logger.log(Level.WARNING, "Problem Updating ElasticSearch ", e);
    }

    public class IndexId {

        final String indexName, id;

        public IndexId(String indexName, String id) {
            this.indexName = indexName;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result
                + ((indexName == null) ? 0 : indexName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof IndexId)) {
                return false;
            }
            IndexId other = (IndexId) obj;
            if (!getOuterType().equals(other.getOuterType())) {
                return false;
            }
            if (id == null) {
                if (other.id != null) {
                    return false;
                }
            } else if (!id.equals(other.id)) {
                return false;
            }
            if (indexName == null) {
                return other.indexName == null;
            } else {
                return indexName.equals(other.indexName);
            }
        }

        private ElasticSearchHandler getOuterType() {
            return ElasticSearchHandler.this;
        }

        @Override
        public String toString() {
            return "IndexId [indexName=" + indexName + ", id=" + id + "]";
        }
    }
}
