package com.tct;

import com.alibaba.nacos.spring.context.annotation.config.NacosPropertySource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
@NacosPropertySource(dataId = "demo", autoRefreshed = true)
public class Application {

   public static void main(String[] args) {
      SpringApplication.run(Application.class, args);
   }

}
