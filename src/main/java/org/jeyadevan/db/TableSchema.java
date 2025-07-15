package org.jeyadevan.db;

import java.util.List;

public class TableSchema {
    public final String tableName;
    public final List<ColumnDef> columns;
    public final int rootPageNo;

    public TableSchema(String tableName, List<ColumnDef> columns, int rootPageNo){
        this.tableName = tableName;
        this.columns = columns;
        this.rootPageNo = rootPageNo;
    }
}
