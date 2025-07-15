package org.jeyadevan.bptree;

import java.util.List;
import java.util.ArrayList;

public class Node {
    public boolean isLeaf;

    public List<byte[]> keys = new ArrayList<>();
    public List<byte[]> values = new ArrayList<>();

    public int nextPage = -1;

    public List<Integer> childrens = new ArrayList<>();

    public Node(boolean isLeaf){
        this.isLeaf = isLeaf;
    }
}
