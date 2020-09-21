package com.tct.config;


import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "hbase")
// @PropertySource(value = {"classpath:config/teacher.properties"})
// @Value

public class HbaseConfig {

   private Map<String, String> config;

   /**
    * 获取配置文件配置信息生成Configuration对象
    * @return hbase配置对象
    * */
   public Configuration configuration() {
      org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
      Set<String> keySet = config.keySet();

      for(String key : keySet) {
         configuration.set(key, config.get(key));
      }

      return configuration;
   }

   /**
    * 加载hbase配置生成链接对象
    * @return hbase链接实例
    * */
   @Bean
   public Connection hbaseConnection() throws Exception {
      Configuration conf = configuration();
      Connection connection = ConnectionFactory.createConnection(conf);
      return connection;
   }

}
