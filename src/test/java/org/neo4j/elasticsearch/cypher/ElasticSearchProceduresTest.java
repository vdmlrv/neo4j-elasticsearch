package org.neo4j.elasticsearch.cypher;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.elasticsearch.ElasticSearchSettings;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

@TestInstance(Lifecycle.PER_CLASS)
public class ElasticSearchProceduresTest {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();

    private static final String LABEL = "MyLabel";
    private static final String INDEX = "my_index";
    private static final String INDEX_SPEC = INDEX + ":" + LABEL + "(foo)";

    private Driver driver;
    private Neo4j embeddedDatabaseServer;

    private JestClient client;

    @BeforeAll
    public void setUp() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
            .Builder("http://localhost:9200")
            .build());
        client = factory.getObject();

        embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
            .withConfig(ElasticSearchSettings.hostName, "http://localhost:9200")
            .withConfig(ElasticSearchSettings.indexSpec, INDEX_SPEC)
            .withConfig(ElasticSearchSettings.enableAutoIndex, Boolean.FALSE)
            .withProcedure(ElasticSearchProcedures.class)
            .withDisabledServer()
            .build();

        driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
    }

    @AfterAll
    public void tearDown() throws IOException {
        client.close();
        driver.close();
        embeddedDatabaseServer.close();
    }

    @Test
    public void testIndex() throws IOException {
        String id;
        try (Session session = driver.session()) {
            session.run("CREATE (:MyLabel{foo: 'bar'})");
            id = String.valueOf(
                session.run("MATCH (n:MyLabel) RETURN ID(n)").next().get(0).asLong());
            session.run("MATCH (n:MyLabel)\n"
                + "CALL es.index(n)");
        }
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());

        assertTrue(response.isSucceeded(), "request failed " + response.getErrorMessage());
        assertEquals(INDEX, response.getValue("_index"));
        assertEquals(id, response.getValue("_id"));
        assertEquals(LABEL, response.getValue("_type"));

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(asList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertEquals("bar", source.get("foo"));
    }

}
