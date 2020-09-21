package com.tct.kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

   private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class.getPackage().getName());

   @KafkaListener(id = "tct_consumer",groupId = "group2",topics = "tct_topic")
   public void listen1(String foo) {
      logger.info("message content [{}]", foo);
   }

}
