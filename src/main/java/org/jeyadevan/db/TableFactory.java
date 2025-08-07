package org.jeyadevan.db;

import org.jeyadevan.bptree.BPlusTree;
import org.jeyadevan.catalog.CatalogStorage;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;

import java.util.List;

public class TableFactory {
    public static Table createNew(String tableName, List<ColumnDef> cols, PageManager pageManager, CatalogStorage catalog) throws Exception{
        int rootPage = pageManager.allocatePage();
        TableSchema schema = new TableSchema(tableName, cols, rootPage);
        System.out.println("RootPage: " + rootPage);
        BPlusTree tree = new BPlusTree(pageManager, rootPage, schema, Constants.PAGE_SIZE, true);
        catalog.set(schema);
        return new Table(schema, tree);
    }

    public static Table load(String tableName, PageManager pageManager, CatalogStorage catalog) throws Exception{
        TableSchema schema = catalog.get(tableName);
        System.out.println("After laoding: " + schema.rootPageNo);
        BPlusTree tree = new BPlusTree(pageManager, schema.rootPageNo, schema ,Constants.PAGE_SIZE, false);
        return new Table(schema, tree);
    }
}
