package org.jeyadevan;

import org.jeyadevan.bptree.BPlusTree;
import org.jeyadevan.catalog.CatalogStorage;
import org.jeyadevan.db.ColumnDef;
import org.jeyadevan.db.DataType;
import org.jeyadevan.db.TableSchema;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;
import org.jeyadevan.db.Row;


import java.io.File;
import java.util.Arrays;
import java.util.List;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        File dataFile = new File(Constants.DATA_FILE);
        File catalogFile = new File(Constants.CATALOG_FILE);

        PageManager pageManager = new PageManager(dataFile);
        CatalogStorage catalog = new CatalogStorage(catalogFile, pageManager);

        TableSchema schema = new TableSchema(
                "admins",
                Arrays.asList(new ColumnDef("id", DataType.INT),
                        new ColumnDef("name", DataType.STRING)),
                0
        );

        int rootPage = pageManager.allocatePage();

        BPlusTree tree = new BPlusTree(pageManager, rootPage, schema, Constants.PAGE_SIZE);

        for (int i=0; i<=3; i++){
            Row row = new Row(List.of(i, "Name" + i));
            tree.insert(row);
        }

        catalog.set(schema);

        TableSchema loadedSchema = catalog.get("admins");

        System.out.println("Loaded Table: " + loadedSchema.tableName);
        for (ColumnDef col : loadedSchema.columns) {
            System.out.println("  - " + col.name + " : " + col.type);
        }

        loadedSchema.printAllRows(pageManager);

        System.out.println("This is a DB Implementation from scratch");
    }

}