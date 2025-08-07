package org.jeyadevan;

import org.jeyadevan.catalog.CatalogStorage;
import org.jeyadevan.db.ColumnDef;
import org.jeyadevan.db.DataType;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;
import org.jeyadevan.db.Row;
import org.jeyadevan.db.TableFactory;
import org.jeyadevan.db.Table;


import java.io.File;
import java.util.List;



public class Main {
    public static void main(String[] args) throws Exception {
        File dataFile = new File(Constants.DATA_FILE);
        File catalogFile = new File(Constants.CATALOG_FILE);

        PageManager pageManager = new PageManager(dataFile);
        CatalogStorage catalog = new CatalogStorage(catalogFile, pageManager);

        // Create a new table
        List<ColumnDef> columns = List.of(
                new ColumnDef("id", DataType.INT),
                new ColumnDef("name", DataType.STRING)
        );
        Table table = TableFactory.createNew("admins", columns, pageManager, catalog);

        for (int i = 0; i <= 3; i++) {
            Row row = new Row(List.of(i, "Name " + i));
            table.insert(row);
        }

        // Load table from catalog and print
        Table loaded = TableFactory.load("admins", pageManager, catalog);
        System.out.println("Loaded Table: " + loaded.getSchema().tableName);
        for (ColumnDef col : loaded.getSchema().columns) {
            System.out.println("  - " + col.name + " : " + col.type);
        }

        System.out.println("All rows");
        loaded.getAllRows();
        System.out.println("2 rows");
        loaded.getAllRows(2);
        System.out.println("row where key -> 1");
        loaded.searchByKey(1);
        System.out.println("Search By value -> name 2");
        loaded.searchByValue("Name 2", "name");
        System.out.println("This is a DB Implementation from scratch");
    }
}