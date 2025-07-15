package org.jeyadevan.catalog;

import org.jeyadevan.db.TableSchema;

public interface ICatalogInterface {
    // Maps Table Name to TableSchemaPage.
    TableSchema get(String tableName);
    void set(TableSchema schema);
}
