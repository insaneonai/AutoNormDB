package org.jeyadevan.catalog;

import org.jeyadevan.db.TableSchema;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;

import org.jeyadevan.util.Serializer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.nio.charset.StandardCharsets;

public class CatalogStorage implements ICatalogInterface {
    private final RandomAccessFile catalogFile;
    private final PageManager pageManager;

    public CatalogStorage(File catalogFile, PageManager pageManager) throws IOException {
        this.catalogFile = new RandomAccessFile(catalogFile, "rw");
        this.pageManager = pageManager;
    }

    @Override
    public TableSchema get(String tableName) {
        try{
            catalogFile.seek(0);
            byte[] nameBuf = new byte[Constants.MAX_NAME_LENGTH];

            while (catalogFile.getFilePointer() < catalogFile.length()){
                catalogFile.readFully(nameBuf);
                String name = new String(nameBuf, StandardCharsets.UTF_8).trim();
                int pageNo = catalogFile.readInt();
                int length = catalogFile.readInt();

                if (name.equals(tableName)){
                    byte[] data = pageManager.readPage(pageNo, length);
                    return Serializer.deserializeSchema(data);
                }
            }

            return null;
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(TableSchema schema) {
        try{
            byte[] serialized = Serializer.serializeSchema(schema);
            int pageNo = pageManager.allocatePage();
            pageManager.writePage(pageNo, serialized);

            byte[] schemaName = schema.tableName.getBytes(StandardCharsets.UTF_8);
            byte[] fixedName = new byte[Constants.MAX_NAME_LENGTH];


            // Truncate Name if exceeds 64 bytes;
            System.arraycopy(schemaName, 0, fixedName, 0, Math.min(schemaName.length, Constants.MAX_NAME_LENGTH));
            catalogFile.seek(catalogFile.length());
            catalogFile.write(fixedName);
            catalogFile.writeInt(pageNo);
            catalogFile.writeInt(serialized.length);

        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
