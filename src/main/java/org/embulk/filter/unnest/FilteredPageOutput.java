package org.embulk.filter.unnest;

import org.apache.commons.lang3.NotImplementedException;
import org.embulk.config.ConfigException;
import org.embulk.filter.unnest.UnnestFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;

class ColumnVisitorImpl implements ColumnVisitor {

    private final PageBuilder pageBuilder;
    private final PageReader pageReader;
    private final Column targetColumn;

    ColumnVisitorImpl(PageBuilder pageBuilder, PageReader pageReader, Column targetColumn) {
        this.pageBuilder = pageBuilder;
        this.pageReader = pageReader;
        this.targetColumn = targetColumn;
    }

    @Override
    public void booleanColumn(Column column) {
        if (pageReader.isNull(column))
            pageBuilder.setNull(column);
        else {
            if (!column.equals(targetColumn))
                pageBuilder.setBoolean(column, pageReader.getBoolean(column));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (pageReader.isNull(column))
            pageBuilder.setNull(column);
        else {
            if (!column.equals(targetColumn))
                pageBuilder.setLong(column, pageReader.getLong(column));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (pageReader.isNull(column))
            pageBuilder.setNull(column);
        else {
            if (!column.equals(targetColumn))
                pageBuilder.setNull(column);
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (pageReader.isNull(column))
            pageBuilder.setNull(column);
        else {
            if (!column.equals(targetColumn))
                pageBuilder.setString(column, pageReader.getString(column));
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (pageReader.isNull(column))
            pageBuilder.setNull(column);
        else {
            if (!column.equals(targetColumn))
                pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (pageReader.isNull(column))
            pageBuilder.setNull(column);
        else {
            if (!column.equals(targetColumn))
                pageBuilder.setJson(column, pageReader.getJson(column));
        }
    }
}

public class FilteredPageOutput implements PageOutput {

    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageBuilder pageBuilder;
    private final PageReader pageReader;
    private final ColumnVisitorImpl visitor;
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
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.pageReader = new PageReader(inputSchema);
        this.outputSchema = outputSchema;

        Column targetColumn = inputSchema.getColumn(targetColumnIndex);
        visitor = new ColumnVisitorImpl(this.pageBuilder, this.pageReader, targetColumn);
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);

        while (pageReader.nextRecord()) {
            outputSchema.visitColumns(visitor);

            Column targetColumn = inputSchema.getColumn(targetColumnIndex);

            // TODO: what to do if value is null?
            for (Value value : pageReader.getJson(targetColumnIndex).asArrayValue()) {
                if ("string".equals(task.getValueType()))
                    pageBuilder.setString(targetColumn, value.toString());
                else if ("boolean".equals(task.getValueType()))
                    pageBuilder.setBoolean(targetColumn, value.asBooleanValue().getBoolean());
                else if ("double".equals(task.getValueType()))
                    pageBuilder.setDouble(targetColumn, value.asFloatValue().toDouble());
                else if ("long".equals(task.getValueType()))
                    pageBuilder.setLong(targetColumn, value.asIntegerValue().toLong());
                else if ("timestamp".equals(task.getValueType()))
                    throw new NotImplementedException("Not implemented");
                else if ("json".equals(task.getValueType()))
                    pageBuilder.setJson(targetColumn, value);
                else
                    throw new ConfigException("Unknown type");

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
        pageBuilder.close();
    }

}