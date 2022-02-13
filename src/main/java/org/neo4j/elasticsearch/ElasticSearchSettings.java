package org.neo4j.elasticsearch;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.STRING;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

@ServiceProvider
public class ElasticSearchSettings implements SettingsDeclaration {

    public static Setting<String> hostName = newBuilder("elasticsearch.host_name",
        STRING, null).build();
    public static Setting<String> indexSpec = newBuilder("elasticsearch.index_spec",
        STRING, null).build();
    public static Setting<Boolean> discovery = newBuilder("elasticsearch.discovery",
        BOOL, Boolean.FALSE).build();
    public static Setting<Boolean> includeIDField = newBuilder("elasticsearch.include_id_field",
        BOOL, Boolean.TRUE).build();
    public static Setting<Boolean> includeLabelsField = newBuilder(
        "elasticsearch.include_labels_field",
        BOOL, Boolean.TRUE).build();
    public static Setting<Boolean> enableAutoIndex = newBuilder("elasticsearch.enable_auto_index",
        BOOL, Boolean.TRUE).build();
    // todo settings for label, property, indexName
}
