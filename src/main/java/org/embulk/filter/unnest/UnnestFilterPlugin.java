package org.embulk.filter.unnest;

import com.google.common.collect.ImmutableList;

import org.embulk.config.Config;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

public class UnnestFilterPlugin implements FilterPlugin {
    public interface PluginTask extends Task {
        @Config("json_column_name")
        String getJsonColumnName();

        @Config("value_type")
        String getValueType();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = buildOutputSchema(task, inputSchema);

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new FilteredPageOutput(task, inputSchema, outputSchema, output);
    }

    private Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0;
        for (Column column : inputSchema.getColumns()) {
            if (column.getName().equals(task.getJsonColumnName())) {
                if ("string".equals(task.getValueType()))
                    builder.add(new Column(i++, column.getName(), Types.STRING));
                else if ("boolean".equals(task.getValueType()))
                    builder.add(new Column(i++, column.getName(), Types.BOOLEAN));
                else if ("double".equals(task.getValueType()))
                    builder.add(new Column(i++, column.getName(), Types.DOUBLE));
                else if ("long".equals(task.getValueType()))
                    builder.add(new Column(i++, column.getName(), Types.LONG));
                else if ("timestamp".equals(task.getValueType()))
                    builder.add(new Column(i++, column.getName(), Types.TIMESTAMP));
                else // Json type
                    builder.add(new Column(i++, column.getName(), Types.JSON));
            } else {
                builder.add(new Column(i++, column.getName(), column.getType()));
            }
        }

        return new Schema(builder.build());
    }
}
