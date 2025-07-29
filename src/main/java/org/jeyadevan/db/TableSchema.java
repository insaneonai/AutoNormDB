package org.jeyadevan.db;

import org.jeyadevan.bptree.Node;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;
import org.jeyadevan.util.NodeSerializer;
import org.jeyadevan.util.RowSerializer;

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

    public void printAllRows(PageManager pageManager) throws Exception{
        int pageNo = this.rootPageNo;

        byte[] indexData = pageManager.readPage(pageNo, Constants.PAGE_SIZE);

        Node node = NodeSerializer.deserializeNode(indexData);

        while (!node.isLeaf) {
            pageNo = node.childrens.getFirst();
            node = NodeSerializer.deserializeNode(pageManager.readPage(pageNo, Constants.PAGE_SIZE));
        }

        while (true) {
            for (byte[] rowBytes: node.values){
                Row row = RowSerializer.deserialize(rowBytes, this);
                System.out.println(row.values);
            }

            if (node.nextPage == -1) break;

            pageNo = node.nextPage;
            node = NodeSerializer.deserializeNode(pageManager.readPage(pageNo, Constants.PAGE_SIZE));
        }
    }
}
