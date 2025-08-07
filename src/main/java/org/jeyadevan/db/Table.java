package org.jeyadevan.db;

import org.jeyadevan.bptree.BPlusTree;

import java.util.List;

public class Table {
    private final TableSchema schema;

    private final BPlusTree tree;

    public Table(TableSchema schema, BPlusTree tree) {
        this.schema = schema;
        this.tree = tree;
    }

    public void insert(Row row) throws Exception {
        tree.insert(row);
    }

    public void getAllRows() throws Exception{
        List<Row> rows = tree.getAll();

        for (Row row: rows){
            System.out.println(row.values);
        }
    }

    public void getAllRows(int limit) throws Exception{
        List<Row> rows = tree.getAll(limit);

        for (Row row: rows){
            System.out.println(row.values);
        }
    }

    public void searchByKey(Object key) throws Exception{
        Row row = tree.searchByKey(key);
        if (row != null) {
            System.out.println(row.values);
        }
        else{
            System.out.println("Can't fetch row with the given key :" + key);
        }
    }

    public void searchByValue(Object value, String column) throws Exception {
        int colIndex = -1;

        for (int i=0; i<schema.columns.size(); i++){
            if (schema.columns.get(i).name.equals(column)){
                colIndex = i;
                break;
            }
        }

        if (colIndex == -1){
            throw new RuntimeException(String.format("Given Column %s does not exist in table %s.", column, schema.tableName));
        }

        List<Row> rows = tree.searchByValue(value, colIndex);

        for (Row row: rows){
            System.out.println(row.values);
        }

    }

    public TableSchema getSchema(){
        return schema;
    }

}
