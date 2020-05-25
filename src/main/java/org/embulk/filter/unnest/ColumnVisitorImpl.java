package org.embulk.filter.unnest;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

class ColumnVisitorImpl implements ColumnVisitor
{
    private final PageBuilder pageBuilder;
    private final PageReader pageReader;
    private final Column targetColumn;

    ColumnVisitorImpl(PageBuilder pageBuilder, PageReader pageReader, Column targetColumn)
    {
        this.pageBuilder = pageBuilder;
        this.pageReader = pageReader;
        this.targetColumn = targetColumn;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (!column.equals(targetColumn)) {
                pageBuilder.setBoolean(column, pageReader.getBoolean(column));
            }
        }
    }

    @Override
    public void longColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (!column.equals(targetColumn)) {
                pageBuilder.setLong(column, pageReader.getLong(column));
            }
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (!column.equals(targetColumn)) {
                pageBuilder.setNull(column);
            }
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (!column.equals(targetColumn)) {
                pageBuilder.setString(column, pageReader.getString(column));
            }
        }
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (!column.equals(targetColumn)) {
                pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
            }
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        if (pageReader.isNull(column)) {
            pageBuilder.setNull(column);
        }
        else {
            if (!column.equals(targetColumn)) {
                pageBuilder.setJson(column, pageReader.getJson(column));
            }
        }
    }
}
