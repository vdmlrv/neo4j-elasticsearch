package org.neo4j.elasticsearch;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

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
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

@TestInstance(Lifecycle.PER_CLASS)
public class ElasticSearchEventHandlerIntegrationTest {

    public static final String LABEL = "MyLabel";
    public static final String INDEX = "my_index";
    public static final String INDEX_SPEC = INDEX + ":" + LABEL + "(foo)";
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
            .withExtensionFactories(List.of(new ElasticSearchKernelExtensionFactory()))
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
            org.neo4j.graphdb.Node node = tx.createNode(Label.label(LABEL));
            id = String.valueOf(node.getId());
            node.setProperty("foo", "foobar");
            tx.commit();
        }

        Thread.sleep(1000); // wait for the async elasticsearch query to complete

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());

        assertEquals(true, response.isSucceeded(), "request failed " + response.getErrorMessage());
        assertEquals(INDEX, response.getValue("_index"));
        assertEquals(id, response.getValue("_id"));
        assertEquals(LABEL, response.getValue("_type"));

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("foobar", source.get("foo"));
    }
}
