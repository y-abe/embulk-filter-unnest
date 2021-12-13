package org.embulk.filter.unnest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.unnest.UnnestFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin.Control;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.value.ValueFactory;

import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.junit.Assert.assertEquals;

public class TestUnnestFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConfigSource getConfigFromYaml(String yaml)
    {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    @Test
    public void testThrowExceptionAbsentJsonColumnName()
    {
        String configYaml = "" + "type: unnest";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'json_column_name' is required but not set");
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void testSchema()
    {
        String configYaml = "" + "type: unnest\n" + "json_column_name: _c0";
        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder().add("_c0", JSON).add("_c1", STRING).add("_c2", DOUBLE).build();

        UnnestFilterPlugin plugin = new UnnestFilterPlugin();
        plugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                assertEquals(3, outputSchema.getColumnCount());

                for (int i = 0; i < 3; i++) {
                    assertEquals(inputSchema.getColumn(i).getName(), outputSchema.getColumn(i).getName());
                    assertEquals(inputSchema.getColumn(i).getType(), outputSchema.getColumn(i).getType());
                }
            }
        });
    }

    @Test
    public void testValues()
    {
        String configYaml = "" + "type: unnest\n" + "json_column_name: _c0";
        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder().add("_c0", JSON).add("_c1", STRING).add("_c2", DOUBLE).build();

        UnnestFilterPlugin plugin = new UnnestFilterPlugin();
        plugin.transaction(config, inputSchema, new Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                MockPageOutput mockPageOutput = new MockPageOutput();

                try (PageOutput pageOutput = plugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            ValueFactory.newArray(ValueFactory.newString("a1"), ValueFactory.newString("a2")),
                            "b",
                            0.5)) {
                        System.out.println(page);
                        pageOutput.add(page);
                    }

                    pageOutput.finish();
                    pageOutput.close();

                    PageReader pageReader = new PageReader(outputSchema);

                    for (Page page : mockPageOutput.pages) {
                            pageReader.setPage(page);
                        int n = 1;
                        while (pageReader.nextRecord()) {
                            assertEquals(String.format("a%d", n++), pageReader.getJson(outputSchema.getColumn(0)).toString());
                            assertEquals("b", pageReader.getString(outputSchema.getColumn(1)));
                            assertEquals(0.5, pageReader.getDouble(outputSchema.getColumn(2)), 0);
                        }
                    }
                }
            }
        });
    }

    private String convertToJsonString(Object object)
    {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
