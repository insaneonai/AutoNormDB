package org.jeyadevan.bptree;

public class InsertResult {
    public byte[] middleKey;
    public int newChild;

    InsertResult(byte[] middleKey, int newChild){
        this.middleKey = middleKey;
        this.newChild = newChild;
    }
}
