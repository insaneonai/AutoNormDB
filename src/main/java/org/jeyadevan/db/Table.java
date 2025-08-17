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

    public void updateByKey(Object key, String column, Object value) throws Exception{
        int colIndex = -1;

        for (int i=0; i<schema.columns.size(); i++){
            if (schema.columns.get(i).name.equals(column)){
                colIndex = i;
                break;
            }
        }

        boolean result = tree.updateByKey(key, colIndex, value);

        if (result){
            System.out.println("Table updated.");
        }
        else{
            System.out.println("No matching row found to update.");
        }
    }

    public void updateByValue(Object key, String keyColumn, Object value, String valueColumn) throws Exception{
        int keyColIndex = -1;
        int valueColIndex = -1;

        for (int i=0; i<schema.columns.size(); i++){
            if (schema.columns.get(i).name.equals(keyColumn)){
                keyColIndex = i;
                break;
            }
        }

        for (int i=0; i<schema.columns.size(); i++){
            if (schema.columns.get(i).name.equals(valueColumn)){
                valueColIndex = i;
                break;
            }
        }

        boolean result = tree.updateByValue(key, keyColIndex, value, valueColIndex);

        if (result){
            System.out.println("Table updated.");
        }
        else{
            System.out.println("No matching row found to update.");
        }
    }

    public TableSchema getSchema(){
        return schema;
    }

}
