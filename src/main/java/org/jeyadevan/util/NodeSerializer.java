package org.jeyadevan.util;

import org.jeyadevan.bptree.Node;
import org.jeyadevan.storage.Constants;

import java.io.*;

public class NodeSerializer {
    public static byte[] serializeNode(Node node) throws IOException {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(boas);

        dos.writeBoolean(node.isLeaf);
        dos.writeInt(node.keys.size());

        for (byte[] key: node.keys){
            dos.writeInt(key.length);
            dos.write(key);
        }


        if (node.isLeaf) {
            for (byte[] value : node.values) {
                dos.writeInt(value.length);
                dos.write(value);
            }
            dos.writeInt(node.nextPage);
        }
        else{
            for (int child: node.childrens){
                dos.writeInt(child);
            }
        }

        dos.flush();

        byte[] data = boas.toByteArray();

        if (data.length > Constants.PAGE_SIZE){
            throw new IOException("Page limit Exceeded");
        }

        byte[] page = new byte[Constants.PAGE_SIZE];

        System.arraycopy(data, 0, page, 0, data.length);

        return page;
    }

    public static Node deserializeNode(byte[] data) throws IOException{
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        boolean isLeaf = dis.readBoolean();
        Node node = new Node(isLeaf);

        int nKeys = dis.readInt();

        for (int i=0; i<nKeys; i++){
            int keyLen = dis.readInt();
            byte[] key = new byte[keyLen];
            dis.readFully(key);
            node.keys.add(key);
        }

        if (isLeaf){
            for (int i=0; i<nKeys; i++){
                int valLen = dis.readInt();
                byte[] value = new byte[valLen];
                dis.readFully(value);
                node.values.add(value);
            }
            node.nextPage = dis.readInt();
        }
        else{
            for (int i=0; i<nKeys + 1; i++){
                node.childrens.add(dis.readInt());
            }
        }

        return node;
    }


}
