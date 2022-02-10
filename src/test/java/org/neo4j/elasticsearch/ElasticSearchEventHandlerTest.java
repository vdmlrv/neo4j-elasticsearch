package org.neo4j.elasticsearch;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

@TestInstance(Lifecycle.PER_CLASS)
public class ElasticSearchEventHandlerTest {

    public static final String INDEX = "test-index";
    public static final String LABEL = "Label";
    private ElasticSearchEventHandler handler;
    private ElasticSearchIndexSettings indexSettings;
    private Neo4j embeddedDatabaseServer;
    private GraphDatabaseService db;
    private DatabaseManagementService dms;
    private JestClient client;
    private Node node;
    private String id;

    @BeforeAll
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
            .Builder("http://localhost:9200")
            .multiThreaded(true)
            .build());
        client = factory.getObject();

        embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
            .withDisabledServer()
            .build();

        dms = embeddedDatabaseServer.databaseManagementService();
        db = embeddedDatabaseServer.defaultDatabaseService();

        Map<String, List<ElasticSearchIndexSpec>> indexSpec =
            ElasticSearchIndexSpecParser.parseIndexSpec(INDEX + ":" + LABEL + "(foo)");
        indexSettings = new ElasticSearchIndexSettings(indexSpec, true, true);

        handler = new ElasticSearchEventHandler(client, indexSettings);
        handler.setUseAsyncJest(false); // don't use async Jest for testing
        dms.registerTransactionEventListener(db.databaseName(), handler);

        // create index
        client.execute(new CreateIndex.Builder(INDEX).build());

    }

    @AfterAll
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.close();

        dms.unregisterTransactionEventListener(db.databaseName(), handler);
        dms.shutdown();
        embeddedDatabaseServer.close();
    }

    private Node createNode() {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(Label.label(LABEL));
            node.setProperty("foo", "bar");
            tx.commit();
            id = String.valueOf(node.getId());
            return node;
        }
    }

    private void assertIndexCreation(JestResult response) throws java.io.IOException {
        client.execute(new Get.Builder(INDEX, id).build());
        assertEquals(true, response.isSucceeded());
        assertEquals(INDEX, response.getValue("_index"));
        assertEquals(id, response.getValue("_id"));
        assertEquals(LABEL, response.getValue("_type"));
    }

    @Test
    public void testAfterCommit() throws Exception {
        node = createNode();
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(singletonList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("bar", source.get("foo"));
    }

    @Test
    public void testAfterCommitWithoutID() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        indexSettings.setIncludeIDField(false);
        indexSettings.setIncludeLabelsField(true);
        client.execute(new CreateIndex.Builder(INDEX).build());
        node = createNode();

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(singletonList(LABEL), source.get("labels"));
        assertNull(source.get("id"));
        assertEquals("bar", source.get("foo"));
    }

    @Test
    public void testAfterCommitWithoutLabels() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        indexSettings.setIncludeIDField(true);
        indexSettings.setIncludeLabelsField(false);
        client.execute(new CreateIndex.Builder(INDEX).build());
        node = createNode();

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(null, source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("bar", source.get("foo"));
    }

    @Test
    public void testUpdate() throws Exception {
        node = createNode();
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        assertEquals("bar", response.getSourceAsObject(Map.class).get("foo"));

        try (Transaction tx = db.beginTx()) {
            node = tx.getNodeById(Integer.parseInt(id));
            node.setProperty("foo", "quux");
            tx.commit();
        }

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        assertEquals(true, response.isSucceeded());
        assertEquals(true, response.getValue("found"));
        assertEquals("quux", response.getSourceAsObject(Map.class).get("foo"));
    }

    @Test
    public void testDelete() throws Exception {
        node = createNode();
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        try (Transaction tx = db.beginTx()) {
            node = tx.getNodeById(Integer.parseInt(id));
            assertEquals("bar",
                node.getProperty("foo")); // check that we get the node that we just added
            node.delete();
            tx.commit();
        }

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        assertEquals(false, response.getValue("found"));
    }
}
