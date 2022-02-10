package org.neo4j.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.searchbox.client.config.HttpClientConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JestDefaultHttpConfigFactoryTest {

    private static HttpClientConfig subject;

    @BeforeEach
    public void beforeEach() throws Throwable {
        subject = JestDefaultHttpConfigFactory.getConfigFor("http://localhost:9200", true);
    }

    @Test
    public void itHasTheCorrectHostName() {
        Set<String> expected = new HashSet<String>(Arrays.asList("http://localhost:9200"));
        assertEquals(expected, subject.getServerList());
    }

    @Test
    public void itIsMultiThreaded() {
        assertTrue(subject.isMultiThreaded());
    }

    @Test
    public void itEnablesDiscovery() {
        assertTrue(subject.isDiscoveryEnabled());
    }

    @Test
    public void itDiscoversEveryOne() {
        final Long one = 1L;
        assertEquals(one, subject.getDiscoveryFrequency());
    }

    @Test
    public void itUsesTheMinuteAsTheDiscoveryUnit() {
        assertEquals(TimeUnit.MINUTES, subject.getDiscoveryFrequencyTimeUnit());
    }

    @Test
    public void itDefaultsToHttp() {
        assertEquals("http://", subject.getDefaultSchemeForDiscoveredNodes());
    }

    @Test
    public void itCanSSL() throws Throwable {
        subject = JestDefaultHttpConfigFactory.getConfigFor("https://localhost:9200", true);

        assertEquals("https://", subject.getDefaultSchemeForDiscoveredNodes());
    }
}
