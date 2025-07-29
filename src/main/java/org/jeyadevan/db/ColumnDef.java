package org.jeyadevan.db;

import java.util.Objects;

public class ColumnDef {
    public final String name;
    public final DataType type;

    public ColumnDef(String name, DataType type){
        this.name = name;
        this.type = type;
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;

        if (!(o instanceof ColumnDef)) return false;

        ColumnDef cdef = (ColumnDef) o;

        return Objects.equals(cdef.name, this.name) && Objects.equals(cdef.type, this.type);
    }
}
