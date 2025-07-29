package org.jeyadevan.bptree;

import org.jeyadevan.db.ColumnDef;
import org.jeyadevan.db.DataType;
import org.jeyadevan.db.TableSchema;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.db.Row;
import org.jeyadevan.storage.Constants;
import org.jeyadevan.util.NodeSerializer;
import org.jeyadevan.util.RowSerializer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BPlusTree {
    private final PageManager pageManager;
    private int rootPage;
    private final TableSchema schema;
    private final int pageSize;

    private byte[] extractKey(Row row) throws Exception{
        Object keyObj = row.values.get(0);
        ColumnDef col = schema.columns.get(0);

        if (col.type == DataType.INT){
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt((Integer) keyObj);

            return baos.toByteArray();
        }
        else if (col.type == DataType.STRING){
            return ((String) keyObj).getBytes(StandardCharsets.UTF_8);
        }
        else{
            throw new Exception("Unsupported key type");
        }
    }

    private int findInsertPosition(List<byte[]> keys, byte[] key) {
        int i = 0;
        while (i < keys.size() && compare(keys.get(i), key) < 0){
            i++;
        }
        return i;
    }

    private int findChildIndex(List<byte[]> keys, byte[] key){
        int i = 0;
        while (i < keys.size() && compare(keys.get(i), key) < 0){
            i++;
        }
        return i;
    }

    private int compare(byte[] a, byte[] b){
        return java.util.Arrays.compare(a, b);
    }

    private InsertResult splitLeaf(Node node, int PageNo) throws Exception{
        int mid = node.keys.size() / 2;

        Node rightNode = new Node(true);

        // Move right half keys and values;
        for (int i=mid; i<node.keys.size(); i++){
            rightNode.keys.add(node.keys.get(i));
            rightNode.values.add(node.values.get(i));
        }

        // Shrink current node to leftHalf;

        node.keys = new ArrayList<>(node.keys.subList(0, mid));
        node.values = new ArrayList<>(node.values.subList(0, mid));

        // Allocate new page for right node;

        int rightPage = pageManager.allocatePage();

        // Handle sequential traversal;
        rightNode.nextPage = node.nextPage;
        node.nextPage = rightPage;

        // Write nodes to Disk;
        byte[] leftData = NodeSerializer.serializeNode(node, true);
        byte[] rightData = NodeSerializer.serializeNode(rightNode, true);

        pageManager.writePage(PageNo, leftData);
        pageManager.writePage(rightPage, rightData);

        // Return middle key to parent;

        byte[] middleKey = rightNode.keys.getFirst();

        return new InsertResult(middleKey, rightPage);
    }

    private InsertResult splitInternal(Node node, int PageNo) throws Exception{
        int mid = node.keys.size() / 2;

        byte[] middleKey = node.keys.get(mid);

        Node rightNode = new Node(false);

        for (int i=mid+1; i<node.keys.size(); i++){
            rightNode.keys.add(node.keys.get(i));
        }

        for (int i=mid+1; i<node.childrens.size(); i++){
            rightNode.childrens.add(node.childrens.get(i));
        }

        // Shrink left node;

        node.keys = new ArrayList<>(node.keys.subList(0, mid));
        node.childrens = new ArrayList<>(node.childrens.subList(0, mid + 1));

        // Allocate page;
        int rightPage = pageManager.allocatePage();

        byte[] leftData = NodeSerializer.serializeNode(node, true);
        byte[] rightData = NodeSerializer.serializeNode(rightNode, true);

        pageManager.writePage(PageNo, leftData);
        pageManager.writePage(rightPage, rightData);

        return new InsertResult(middleKey, rightPage);


    }

    public BPlusTree(PageManager pm, int rootPage, TableSchema schema, int pageSize) throws Exception{
        this.pageManager = pm;
        this.rootPage = rootPage;
        this.schema = schema;
        this.pageSize = pageSize;

        Node root = new Node(true);  // isLeaf = true
        byte[] data = NodeSerializer.serializeNode(root, true);
        pageManager.writePage(rootPage, data);
    }

    private int nodeSize(Node node) throws Exception {
        byte[] data = NodeSerializer.serializeNode(node, false);
        return data.length;
    }

    public void insert(Row row) throws Exception{
        byte[] key = extractKey(row);
        byte[] value = RowSerializer.serializeRow(row, schema);

        InsertResult result = insertRecurssive(rootPage, key, value);

        if (result != null && result.newChild != -1){
            // node splitting.
            Node newRoot = new Node(false);
            newRoot.keys.add(result.middleKey);
            newRoot.childrens.add(rootPage);
            newRoot.childrens.add(result.newChild);

            byte[] serialized = NodeSerializer.serializeNode(newRoot, false);

            if (serialized.length > Constants.PAGE_SIZE){
                throw new RuntimeException("Page limit exceeded...");
            }

            byte[] page = new byte[Constants.PAGE_SIZE];

            System.arraycopy(serialized, 0, page, 0, serialized.length);

            int newRootPage = pageManager.allocatePage();
            pageManager.writePage(newRootPage, page);
            rootPage = newRootPage;
        }
    }

    private InsertResult insertInLeaf(Node node, int rootPage, byte[] key, byte[] value) throws Exception{
        int pos = findInsertPosition(node.keys, key);

        node.keys.add(pos, key);
        node.values.add(pos, value);

        if (nodeSize(node) > Constants.PAGE_SIZE){
            return splitLeaf(node, rootPage);
        }
        else{
            byte[] newData = NodeSerializer.serializeNode(node, true);
            pageManager.writePage(rootPage, newData);
            return null;
        }
    }

    private InsertResult insertInInternal(Node node, int rootPage, byte[] key, byte[] value) throws Exception{
        int childIndex = findChildIndex(node.keys, key);
        int childPage = node.childrens.get(childIndex);

        InsertResult childResult = insertRecurssive(childPage, key, value);

        if (childResult != null){
            node.keys.add(childIndex, childResult.middleKey);
            node.childrens.add(childIndex + 1, childResult.newChild);

            if (nodeSize(node) > pageSize){
                return splitInternal(node, rootPage);
            }
        }

        byte[] newData = NodeSerializer.serializeNode(node, true);
        pageManager.writePage(rootPage, newData);

        return null;
    }

    private InsertResult insertRecurssive(int rootPage, byte[] key, byte[] value) throws Exception{
        byte[] pageData = pageManager.readPage(rootPage, Constants.PAGE_SIZE);
        Node node = NodeSerializer.deserializeNode(pageData);

        if (node.isLeaf){
            return insertInLeaf(node, rootPage, key, value);
        }
        else{
            return insertInInternal(node, rootPage, key, value);
        }
    }
}
