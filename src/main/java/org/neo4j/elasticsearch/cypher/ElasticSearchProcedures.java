package org.neo4j.elasticsearch.cypher;

import java.io.IOException;
import org.neo4j.elasticsearch.ElasticSearchHandler;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class ElasticSearchProcedures {

    @Procedure(name = "es.index")
    @Description("Put a node into elasticsearch index")
    public void index(@Name("node") Node node) throws IOException {
        ElasticSearchHandler.getInstance().index(node);
    }

}
