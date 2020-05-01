package org.embulk.filter.unnest;

import org.apache.commons.lang3.NotImplementedException;
import org.embulk.config.ConfigException;
import org.embulk.filter.unnest.UnnestFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;

public class FilteredPageOutput implements PageOutput {

    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private int targetColumnIndex;

    FilteredPageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput pageOutput) {
        boolean targetColumnIndexInitialized = false;
        for (Column column : inputSchema.getColumns()) {
            if (column.getName().equals(task.getJsonColumnName())) {
                targetColumnIndex = column.getIndex();
                if (!Types.JSON.equals(column.getType()))
                    throw new ConfigException(String.format("column %s must be json type", column.getName()));

                targetColumnIndexInitialized = true;
                break;
            }
        }
        if (!targetColumnIndexInitialized)
            throw new ConfigException(String.format("column %s not found", task.getJsonColumnName()));

        this.task = task;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            Column targetColumn = inputSchema.getColumn(targetColumnIndex);
            for (Value value : pageReader.getJson(targetColumn).asArrayValue()) {
                for (Column column : outputSchema.getColumns()) {
                    if (column.getIndex() == targetColumnIndex) {
                        if ("string".equals(task.getValueType()))
                            pageBuilder.setString(column, value.toString());
                        else if ("boolean".equals(task.getValueType()))
                            pageBuilder.setBoolean(column, value.asBooleanValue().getBoolean());
                        else if ("double".equals(task.getValueType()))
                            pageBuilder.setDouble(column, value.asFloatValue().toDouble());
                        else if ("long".equals(task.getValueType()))
                            pageBuilder.setLong(column, value.asIntegerValue().toLong());
                        else if ("timestamp".equals(task.getValueType()))
                            throw new NotImplementedException("sorry");
                        else // Json type
                            throw new NotImplementedException("sorry");
                    } else {
                        if (Types.STRING.equals(column.getType()))
                            pageBuilder.setString(column, pageReader.getString(column));
                        else if (Types.BOOLEAN.equals(column.getType()))
                            pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                        else if (Types.DOUBLE.equals(column.getType()))
                            pageBuilder.setDouble(column, pageReader.getDouble(column));
                        else if (Types.LONG.equals(column.getType()))
                            pageBuilder.setLong(column, pageReader.getLong(column));
                        else if (Types.TIMESTAMP.equals(column.getType()))
                            pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                        else // Json type
                            pageBuilder.setJson(column, pageReader.getJson(column));
                    }
                }
                pageBuilder.addRecord();
            }
        }
    }

    @Override
    public void finish() {
        pageBuilder.finish();
    }

    @Override
    public void close() {
        pageReader.close();
        pageBuilder.close();
    }

}