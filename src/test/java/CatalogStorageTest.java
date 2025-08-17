import org.jeyadevan.catalog.CatalogStorage;
import org.jeyadevan.db.ColumnDef;
import org.jeyadevan.db.DataType;
import org.jeyadevan.db.TableSchema;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;

public class CatalogStorageTest {
    @Test
    void createSchema() throws Exception{
        File dataFile = new File(Constants.DATA_FILE);
        File catalogFile = new File(Constants.CATALOG_FILE);

        UUID uuid = UUID.randomUUID();

        String tableName = uuid.toString();

        PageManager pagemanager = new PageManager(dataFile);

        CatalogStorage catalog = new CatalogStorage(catalogFile, pagemanager);

        TableSchema tableSchema = new TableSchema(tableName,
                Arrays.asList(new ColumnDef("id", DataType.INT),
                        new ColumnDef("name", DataType.STRING)
                        ), 0);

        catalog.set(tableSchema);

        TableSchema loadedSchema = catalog.get(tableName);

        assertEquals(tableSchema.tableName, loadedSchema.tableName);

        assertEquals(tableSchema.rootPageNo, loadedSchema.rootPageNo);

        assertEquals(tableSchema.columns, loadedSchema.columns);

    }
}
