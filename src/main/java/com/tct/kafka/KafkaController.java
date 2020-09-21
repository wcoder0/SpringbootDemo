package com.tct.kafka;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

   @Autowired
   private KafkaTemplate kafkaTemplate;

   private static final Logger logger = LoggerFactory.getLogger(KafkaController.class.getPackage().getName());

   @PostMapping(value="/sendMsg")
   public void sendMsg() {
      kafkaTemplate.send("tct_topic", "haha");
      logger.info("发送消息");
      createConsumer();
   }

   private void createConsumer(){
      String kafkaServer = "123.57.40.94:9092";
      //关联kafka
      Properties props = new Properties();
      props.put("bootstrap.servers", kafkaServer);
      props.put("group.id", "group1");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      //1.创建消费者
      org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

      try {
         //2.订阅Topic
         consumer.subscribe(Arrays.asList("tct_topic"));
         while (true) {
            //3.读取数据
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

            for (ConsumerRecord<String, String> record : records) {

               String str =
                     "====MsgReceiver====topic" + record.topic() + "partition" + record.partition() + "offset" + record.offset()
                     + "offset" + record.offset() + "value" + record.value();
               System.err.println(str);
            }
         }
      } catch (WakeupException e) {
         // ignore for shutdown
         e.printStackTrace();
      } finally {
         //6.退出应用程序前使用close方法关闭消费者，网络连接和socket也会随之关闭，并立即触发一次再均
         consumer.close();
      }
   }

}
