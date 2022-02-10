package org.neo4j.elasticsearch;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.elasticsearch.ElasticSearchIndexSpecParser.parseIndexSpec;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ElasticSearchIndexSpecParserTest {

    @Test
    public void testParseIndexSpec() throws ParseException {
        Map<String, List<ElasticSearchIndexSpec>> rv =
            parseIndexSpec(
                "index_name:Label(foo,bar,quux),other_index_name:OtherLabel(baz,quuxor)");
        assertEquals(2, rv.size());
        assertEquals(new HashSet<>(asList("Label", "OtherLabel")), rv.keySet());
    }

    @Test
    public void testIndexSpecBadSyntax() throws ParseException {
        Map rv = parseIndexSpec("index_name:Label(foo,bar");
        assertEquals(0, rv.size());
        rv = parseIndexSpec("index_name:Label");
        assertEquals(0, rv.size());
        rv = parseIndexSpec("Label");
        assertEquals(0, rv.size());
    }

    @Test
    public void testIndexSpecBadSyntaxDuplicateIndex() throws ParseException {
        assertThrows(ParseException.class, () -> {
            parseIndexSpec("index_name:Label(foo,bar),index_name:Label(quux)");
        });
    }


}
