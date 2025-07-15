package org.jeyadevan.db;

public class ColumnDef {
    public final String name;
    public final DataType type;

    public ColumnDef(String name, DataType type){
        this.name = name;
        this.type = type;
    }
}
