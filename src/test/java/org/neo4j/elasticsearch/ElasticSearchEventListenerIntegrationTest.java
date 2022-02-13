package org.neo4j.elasticsearch;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
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
public class ElasticSearchEventListenerIntegrationTest {

    private static final String LABEL = "MyLabel";
    private static final String INDEX = "my_index";
    private static final String INDEX_SPEC = INDEX + ":" + LABEL + "(foo)";

    private Neo4j embeddedDatabaseServer;
    private GraphDatabaseService db;
    private DatabaseManagementService dms;
    private JestClient client;

    @BeforeAll
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
            .Builder("http://localhost:9200")
            .build());
        client = factory.getObject();

        embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
            .withConfig(ElasticSearchSettings.hostName, "http://localhost:9200")
            .withConfig(ElasticSearchSettings.indexSpec, INDEX_SPEC)
            .withDisabledServer()
            .build();

        dms = embeddedDatabaseServer.databaseManagementService();
        db = embeddedDatabaseServer.defaultDatabaseService();

        // create index
        client.execute(new CreateIndex.Builder(INDEX).build());
    }

    @AfterAll
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.close();

        dms.shutdown();
        embeddedDatabaseServer.close();
    }

    @Test
    public void testAfterCommit() throws Exception {
        String id;
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(Label.label(LABEL));
            id = String.valueOf(node.getId());
            node.setProperty("foo", "foobar");
            tx.commit();
        }

        Thread.sleep(1000); // wait for the async elasticsearch query to complete

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());

        assertTrue(response.isSucceeded(), "request failed " + response.getErrorMessage());
        assertEquals(INDEX, response.getValue("_index"));
        assertEquals(id, response.getValue("_id"));
        assertEquals(LABEL, response.getValue("_type"));

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("foobar", source.get("foo"));
    }

    @Test
    void testCreateMultipleNodes() throws Exception {
        String[] ids = new String[2];
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 2; ++i) {
                Node node = tx.createNode(Label.label(LABEL));
                ids[i] = String.valueOf(node.getId());
                node.setProperty("foo", "bar" + (i + 1));
            }
            tx.commit();
        }
        Thread.sleep(1000);

        for (int i = 0; i < 2; ++i) {
            JestResult response = client.execute(new Get.Builder(INDEX, ids[i]).build());

            assertTrue(response.isSucceeded(), "request failed " + response.getErrorMessage());
            assertEquals(INDEX, response.getValue("_index"));
            assertEquals(ids[i], response.getValue("_id"));
            assertEquals(LABEL, response.getValue("_type"));

            Map source = response.getSourceAsObject(Map.class);
            assertEquals(asList(LABEL), source.get("labels"));
            assertEquals(ids[i], source.get("id"));
            assertEquals("bar" + (i + 1), source.get("foo"));
        }
    }
}
