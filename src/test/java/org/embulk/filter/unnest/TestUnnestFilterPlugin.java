package org.embulk.filter.unnest;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.unnest.UnnestFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestUnnestFilterPlugin
{

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
        String yaml = "" +
                "type: unnest\n" +
                "value_type: string";
        ConfigSource config = getConfigFromYaml(yaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'json_column_name' is required but not set");
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void testThrowExceptionAbsentValueType() {
        String yaml = "" +
                "type: unnest\n" +
                "json_column_name: hoge";
        ConfigSource config = getConfigFromYaml(yaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'value_type' is required but not set");
        config.loadConfig(PluginTask.class);
    }

}
