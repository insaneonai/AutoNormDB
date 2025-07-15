package org.jeyadevan;

import org.jeyadevan.catalog.CatalogStorage;
import org.jeyadevan.db.ColumnDef;
import org.jeyadevan.db.DataType;
import org.jeyadevan.db.TableSchema;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;

import java.io.File;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        File dataFile = new File(Constants.DATA_FILE);
        File catalogFile = new File(Constants.CATALOG_FILE);

        PageManager pageManager = new PageManager(dataFile);
        CatalogStorage catalog = new CatalogStorage(catalogFile, pageManager);

        TableSchema schema = new TableSchema(
                "students",
                Arrays.asList(new ColumnDef("id", DataType.INT),
                        new ColumnDef("name", DataType.STRING)),
                0
        );

        TableSchema schema1 = new TableSchema(
                "courses",
                Arrays.asList(new ColumnDef("id", DataType.INT),
                        new ColumnDef("name", DataType.STRING),
                        new ColumnDef("tutor", DataType.STRING)),
                1
        );

        catalog.set(schema);
        catalog.set(schema1);
        TableSchema loadedSchema = catalog.get("students");
        TableSchema loadedSchema2 = catalog.get("courses");

        System.out.println("Loaded Table: " + loadedSchema.tableName);
        for (ColumnDef col : loadedSchema.columns) {
            System.out.println("  - " + col.name + " : " + col.type);
        }
        System.out.println("Loaded Table: " + loadedSchema2.tableName);
        for (ColumnDef col : loadedSchema2.columns) {
            System.out.println("  - " + col.name + " : " + col.type);
        }

        System.out.println("This is a DB Implementation from scratch");
    }

}