package org.jeyadevan;

import org.jeyadevan.catalog.CatalogStorage;
import org.jeyadevan.db.*;
import org.jeyadevan.io.PageManager;
import org.jeyadevan.storage.Constants;


import javax.xml.crypto.Data;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;



public class Main {
    public static void main(String[] args) throws Exception {
        File dataFile = new File(Constants.DATA_FILE);
        File catalogFile = new File(Constants.CATALOG_FILE);

        PageManager pageManager = new PageManager(dataFile);
        CatalogStorage catalog = new CatalogStorage(catalogFile, pageManager);

        Table table = null;

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n===== MENU =====");
            System.out.println("1. Create new table");
            System.out.println("2. Insert row");
            System.out.println("3. Load table");
            System.out.println("4. View all rows");
            System.out.println("5. View limited rows");
            System.out.println("6. Search by key");
            System.out.println("7. Search by value");
            System.out.println("8. Update by key");
            System.out.println("9. Update by value");
            System.out.println("0. Exit");
            System.out.print("Enter choice: ");

            int choice = Integer.parseInt(scanner.nextLine());

            switch (choice) {
                case 1:
                    System.out.print("Enter table name: ");
                    String tableName = scanner.nextLine();

                    List<ColumnDef> columnsDynamic = new java.util.ArrayList<>();
                    while (true) {
                        System.out.print("Enter column name (or press Enter to finish): ");
                        String colName = scanner.nextLine().trim();
                        if (colName.isEmpty()) break;

                        // Show available types
                        System.out.println("Available data types:");
                        for (DataType dt : DataType.values()) {
                            System.out.println(" - " + dt);
                        }
                        System.out.print("Enter data type: ");
                        String typeStr = scanner.nextLine().trim().toUpperCase();

                        DataType type;
                        try {
                            type = DataType.valueOf(typeStr);
                        } catch (IllegalArgumentException e) {
                            System.out.println("Invalid data type. Try again.");
                            continue;
                        }

                        columnsDynamic.add(new ColumnDef(colName, type));
                    }

                    if (columnsDynamic.isEmpty()) {
                        System.out.println("No columns defined. Table creation cancelled.");
                        break;
                    }

                    table = TableFactory.createNew(tableName, columnsDynamic, pageManager, catalog);
                    System.out.println("Table created: " + tableName);
                    break;

                case 2:
                    if (table == null) {
                        System.out.println("Load or create a table first!");
                        break;
                    }
                    TableSchema schema = table.getSchema();
                    List<ColumnDef> columns = schema.columns;
                    List<Object> rowvalue = new ArrayList<>();


                    for (int i=0; i<columns.size(); i++){
                        ColumnDef cdf = columns.get(i);
                        System.out.print("Please enter value for : " + cdf.name + ": ");
                        if (cdf.type == DataType.INT){
                            int value = scanner.nextInt();
                            rowvalue.add(i, value);
                        }
                        else if (cdf.type == DataType.STRING){
                            String value = scanner.nextLine();
                            rowvalue.add(i, value);
                        }
                    }

                    Row row = new Row(rowvalue);

                    table.insert(row);

                    break;

                case 3:
                    System.out.print("Enter table name to load: ");
                    String loadName = scanner.nextLine();
                    table = TableFactory.load(loadName, pageManager, catalog);
                    System.out.println("Loaded table: " + table.getSchema().tableName);
                    for (ColumnDef col : table.getSchema().columns) {
                        System.out.println("  - " + col.name + " : " + col.type);
                    }
                    break;

                case 4:
                    if (table != null) table.getAllRows();
                    else System.out.println("No table loaded!");
                    break;

                case 5:
                    if (table != null) {
                        System.out.print("Enter limit: ");
                        int limit = Integer.parseInt(scanner.nextLine());
                        table.getAllRows(limit);
                    } else System.out.println("No table loaded!");
                    break;

                case 6:
                    if (table != null) {
                        System.out.print("Enter key: ");
                        int key = Integer.parseInt(scanner.nextLine());
                        table.searchByKey(key);
                    } else System.out.println("No table loaded!");
                    break;

                case 7:
                    if (table != null) {
                        System.out.print("Enter column name: ");
                        String colName = scanner.nextLine();
                        System.out.print("Enter value: ");
                        String val = scanner.nextLine();
                        table.searchByValue(val, colName);
                    } else System.out.println("No table loaded!");
                    break;

                case 8:
                    if (table != null) {
                        System.out.print("Enter key: ");
                        int key = Integer.parseInt(scanner.nextLine());
                        System.out.print("Enter column name: ");
                        String colName = scanner.nextLine();
                        System.out.print("Enter new value: ");
                        String newVal = scanner.nextLine();
                        table.updateByKey(key, colName, newVal);
                    } else System.out.println("No table loaded!");
                    break;

                case 9:
                    if (table != null) {
                        System.out.print("Enter search value: ");
                        String searchVal = scanner.nextLine();
                        System.out.print("Enter search column: ");
                        String searchCol = scanner.nextLine();
                        System.out.print("Enter new value: ");
                        String updVal = scanner.nextLine();
                        System.out.print("Enter update column: ");
                        String updCol = scanner.nextLine();
                        table.updateByValue(searchVal, searchCol, updVal, updCol);
                    } else System.out.println("No table loaded!");
                    break;

                case 0:
                    System.out.println("Exiting...");
                    return;

                default:
                    System.out.println("Invalid choice!");
            }
        }
    }
}