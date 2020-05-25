package org.embulk.filter.unnest;

import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

public class UnnestFilterPlugin implements FilterPlugin {
    public interface PluginTask extends Task {
        @Config("json_column_name")
        String getJsonColumnName();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // Verify if the column specified with json_column_name exists.
        inputSchema.lookupColumn(task.getJsonColumnName());

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new FilteredPageOutput(task, inputSchema, outputSchema, output);
    }
}
