package org.jeyadevan.util;

import org.jeyadevan.db.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Serializer {
    public static byte[] serializeSchema(TableSchema schema) throws IOException{
        /*
        * @param: TableSchema schema: Contains MetaData about a table.
        * @return: byte[]: Serialized schema.
        *
        * Serialization Strategy: TableNameLen + TableName + N cols + colLen + colName + colType + rootPage
        * TotalBytesAllocated: 4 + varLen + N cols * (4 + varLen + 4) + 4;
        * */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        byte[] nameBytes = schema.tableName.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(nameBytes.length);
        dos.write(nameBytes);

        dos.writeInt(schema.columns.size());

        for (ColumnDef col: schema.columns){
            byte[] colNameBytes = col.name.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(colNameBytes.length);
            dos.write(colNameBytes);
            dos.writeInt(col.type.ordinal());
        }

        dos.writeInt(schema.rootPageNo);
        dos.flush();
        return baos.toByteArray();
    }

    public static TableSchema deserializeSchema(byte[] data) throws IOException{
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        int nameLen = dis.readInt();
        byte[] nameBytes = new byte[nameLen];
        dis.readFully(nameBytes);
        String tableName = new String(nameBytes, StandardCharsets.UTF_8);

        int numCols = dis.readInt();
        List<ColumnDef> columns = new ArrayList<>();
        for (int i=0; i<numCols; i++){
            int colNameLen = dis.readInt();
            byte[] colNameBytes = new byte[colNameLen];
            dis.readFully(colNameBytes);
            String colName = new String(colNameBytes, StandardCharsets.UTF_8);
            int typeOrdinal = dis.readInt();
            columns.add(new ColumnDef(colName, DataType.values()[typeOrdinal]));
        }

        int rootPageNo = dis.readInt();
        return new TableSchema(tableName, columns, rootPageNo);


    }
}
