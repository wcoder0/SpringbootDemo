package com.tct.hbase.controller;


import java.util.*;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class EsControlller {


   @Autowired
   private RestHighLevelClient restHighLevelClient;


   @PostMapping("createIndex")
   public void createIndex()throws Exception {
      String indexName = "";
      CreateIndexRequest request = new CreateIndexRequest(indexName);
      request.settings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .build());
      //request.source("", XContentType.JSON);
      String mapping = "";
      request.mapping("user",mapping,XContentType.JSON);
      request.alias(new Alias("user_alias"));
      CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request,RequestOptions.DEFAULT);
   }


   @DeleteMapping("deleteIndex")
   public void deleteIndex()throws Exception {
      String indexName = "";
      DeleteIndexRequest request = new DeleteIndexRequest(indexName);
      restHighLevelClient.indices().delete(request,RequestOptions.DEFAULT);
   }

   @PostMapping("createDocumnt")
   public void createDocumnt()throws Exception{
      IndexRequest indexRequest = new IndexRequest("indexName");
      indexRequest.source("",XContentType.JSON);
      restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
   }


   @GetMapping("getData")
   public void getData()throws Exception{
      GetRequest getRequest = new GetRequest("indexName","user");
      getRequest.id("1");
      GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
      documentFields.getFields();
   }
}
