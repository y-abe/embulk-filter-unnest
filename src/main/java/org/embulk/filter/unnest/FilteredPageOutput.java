package org.embulk.filter.unnest;

import org.embulk.filter.unnest.UnnestFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.msgpack.value.Value;

public class FilteredPageOutput implements PageOutput {

    private final PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageBuilder pageBuilder;
    private final PageReader pageReader;
    private final ColumnVisitorImpl visitor;

    FilteredPageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput pageOutput) {

        this.task = task;
        this.inputSchema = inputSchema;
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.pageReader = new PageReader(inputSchema);
        this.outputSchema = outputSchema;

        visitor = new ColumnVisitorImpl(this.pageBuilder, this.pageReader, inputSchema.lookupColumn(task.getJsonColumnName()));
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);

        while (pageReader.nextRecord()) {
            outputSchema.visitColumns(visitor);

            Column targetColumn = inputSchema.lookupColumn(task.getJsonColumnName());

            for (Value value : pageReader.getJson(targetColumn).asArrayValue()) {
                pageBuilder.setJson(targetColumn, value);
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