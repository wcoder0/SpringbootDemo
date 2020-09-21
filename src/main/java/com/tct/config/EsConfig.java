package com.tct.config;


import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

public class EsConfig {

   @Bean
   @Scope("singleton")
   public RestHighLevelClient initElasticsearch() {
      RestHighLevelClient client;
      HttpHost[] httpHosts = new HttpHost[3];

      client = new RestHighLevelClient(
            RestClient.builder(httpHosts)
      );

      return client;
   }


}
