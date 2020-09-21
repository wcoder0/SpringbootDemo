package com.tct.hbase.controller;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HbaseController {

   @Autowired
   private Connection hbaseConnection;


   @PostMapping("createTable")
   public void createHbaseTable(String tableNameStr, String... columns) {
      Admin admin = null;

      try {
         admin = hbaseConnection.getAdmin();
         TableName tableName = TableName.valueOf(tableNameStr);
         boolean exists = admin.tableExists(tableName);

         if(!exists) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            for(String column : columns) {
               HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(column);
               hTableDescriptor.addFamily(hColumnDescriptor);
            }

            admin.createTable(hTableDescriptor);
         }
      }
      catch(IOException e) {
         logger.warn("创建表失败", e);
      }
      finally {
         try {
            if(admin != null) {
               admin.close();
            }
         }
         catch(IOException e) {
            logger.warn("admin.close()失败", e);
         }
      }
   }


   @PostMapping("insertData")
   public void createHbaseTable(String tableNameStr, String rowKey, String colFamily, String col, String val) {
      Table table = null;

      try {
         TableName tableName = TableName.valueOf(tableNameStr);
         table = hbaseConnection.getTable(tableName);
         Put put = new Put(rowKey.getBytes());
         put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
         table.put(put);
      }
      catch(IOException e) {
         logger.warn("创建表失败", e);
      }
      finally {
         try {
            if(table != null) {
               table.close();
            }
         }
         catch(IOException e) {
            logger.warn("table.close()失败", e);
         }
      }
   }


   @PostMapping("insertDatas")
   public void createHbaseTable(String tableNameStr, List<Map<String, String>> datas) {
      Table table = null;

      try {
         TableName tableName = TableName.valueOf(tableNameStr);
         table = hbaseConnection.getTable(tableName);
         List<Put> puts = new ArrayList<>();

         for(Map<String, String> data : datas) {
            String colFamily = data.get("colFamily");
            String rowKey = data.get("rowKey");
            String col = data.get("col");
            String val = data.get("val");
            Put put = new Put(rowKey.getBytes());
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
            puts.add(put);
            table.put(puts);
         }
      }
      catch(Exception e) {
         logger.warn("创建表失败", e);
      }
      finally {
         try {
            if(table != null) {
               table.close();
            }
         }
         catch(IOException e) {
            logger.warn("table.close()失败", e);
         }
      }
   }


   @PostMapping("insertDatas2")
   public void createHbaseTable(String tableNameStr, String rowKey, String colFamily, List<Map<String, String>> datas) {
      Table table = null;

      try {
         TableName tableName = TableName.valueOf(tableNameStr);
         table = hbaseConnection.getTable(tableName);
         Put put = new Put(rowKey.getBytes());

         for(Map<String, String> data : datas) {
            String col = data.get("col");
            String val = data.get("val");
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
            table.put(put);
         }
      }
      catch(Exception e) {
         logger.warn("创建表失败", e);
      }
      finally {
         try {
            if(table != null) {
               table.close();
            }
         }
         catch(IOException e) {
            logger.warn("table.close()失败", e);
         }
      }
   }


   @PostMapping("getDataByRowkey")
   public void getDataByRowkey(String tableNameStr, String rowKey, String colFamily) {
      Table table = null;

      try {
         TableName tableName = TableName.valueOf(tableNameStr);
         table = hbaseConnection.getTable(tableName);
         Get get = new Get(Bytes.toBytes(rowKey));
         Result result = table.get(get);
         Cell[] cells = result.rawCells();

         for(Cell cell : cells) {
            Bytes.toString(CellUtil.cloneQualifier(cell));
            Bytes.toString(CellUtil.cloneValue(cell));
         }
      }
      catch(Exception e) {
         logger.warn("创建表失败", e);
      }
      finally {
         try {
            if(table != null) {
               table.close();
            }
         }
         catch(IOException e) {
            logger.warn("table.close()失败", e);
         }
      }
   }


   @PostMapping("getData")
   public void getDataScan(String tableNameStr, String rowKey, String colFamily) {
      Table table = null;
      ResultScanner resultScanner = null;

      try {
         TableName tableName = TableName.valueOf(tableNameStr);
         table = hbaseConnection.getTable(tableName);
         Scan scan = new Scan();
         scan.setStartRow(Bytes.toBytes(rowKey));
         resultScanner = table.getScanner(scan);

         for(Result result : resultScanner) {
            List<Cell> cells = result.listCells();

         }

      }
      catch(Exception e) {
         logger.warn("创建表失败", e);
      }
      finally {
         try {
            if(resultScanner != null) {
               resultScanner.close();
            }

            if(table != null) {
               table.close();
            }
         }
         catch(IOException e) {
            logger.warn("table.close()失败", e);
         }
      }
   }


   private static final Logger logger = LoggerFactory.getLogger(HbaseController.class.getPackage().getName());

}
