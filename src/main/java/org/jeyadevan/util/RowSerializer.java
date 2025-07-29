package org.jeyadevan.util;
import org.jeyadevan.db.ColumnDef;
import org.jeyadevan.db.DataType;
import org.jeyadevan.db.Row;
import org.jeyadevan.db.TableSchema;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RowSerializer {
    public static byte[] serializeRow(Row row, TableSchema schema) throws IOException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        List<Object> values = row.values;

        for (int i=0; i<schema.columns.size(); i++){
            ColumnDef col = schema.columns.get(i);
            Object value = values.get(i);

            switch (col.type){
                case DataType.INT:
                    dos.writeInt((Integer) value);
                    break;
                case DataType.STRING:
                    byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(strBytes.length);
                    dos.write(strBytes);
                    break;
                default:
                    break;
            }
        }
        dos.flush();
        return baos.toByteArray();
    }

    public static Row deserialize(byte[] data, TableSchema schema) throws IOException{
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        List<ColumnDef> cols = schema.columns;

        List<Object> values = new ArrayList<>();

        for (ColumnDef col : cols){
            if (col.type == DataType.INT){
                values.add(dis.readInt());
            }
            if (col.type == DataType.STRING){
                int len = dis.readInt();
                byte[] strBytes = new byte[len];
                dis.readFully(strBytes);
                values.add(new String(strBytes, StandardCharsets.UTF_8));
            }
        }

        return new Row(values);
    }
}
