package org.embulk.filter.unnest;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.unnest.UnnestFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.FilterPlugin.Control;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.embulk.spi.type.Types.STRING;

import com.google.common.collect.ImmutableMap;

import static org.embulk.spi.type.Types.JSON;

public class TestUnnestFilterPlugin {

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConfigSource getConfigFromYaml(String yaml) {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    @Test
    public void testThrowExceptionAbsentJsonColumnName() {
        String configYaml = "" + "type: unnest";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'json_column_name' is required but not set");
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void testValues() {
        String configYaml = ""
                          + "type: unnest\n"
                          + "json_column_name: _c0";
        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
            .add("_c0", JSON)
            .add("_c1", STRING)
            .add("_c2", STRING)
            .build();

        UnnestFilterPlugin plugin = new UnnestFilterPlugin();
        plugin.transaction(config, inputSchema, new Control(){
        
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                MockPageOutput mockPageOutput = new MockPageOutput();
                PageOutput pageOutput = plugin.open(taskSource, inputSchema, outputSchema, mockPageOutput);
                ImmutableMap.Builder<String,Object> builder = ImmutableMap.builder();
                builder.put("_c0", "[\"a\", \"b\", \"c\"]");
            }
        });
    }
}
