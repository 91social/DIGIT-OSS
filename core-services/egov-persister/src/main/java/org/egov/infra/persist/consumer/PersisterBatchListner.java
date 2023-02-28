package org.egov.infra.persist.consumer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.egov.infra.persist.service.PersistService;
import org.egov.infra.persist.service.Sql2oPersistService;
import org.egov.tracer.kafka.CustomKafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class PersisterBatchListner implements BatchMessageListener<String, Object> {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Sql2oPersistService persistService;

    @Autowired
    private CustomKafkaTemplate kafkaTemplate;

    @Value("${audit.persist.kafka.topic}")
    private String persistAuditKafkaTopic;

    @Value("${audit.generate.kafka.topic}")
    private String auditGenerateKafkaTopic;

    @Value("${persist.thread.size}")
    private int threadSize;

    private  ExecutorService executorService;
    @PostConstruct
    public void init() {
        this.executorService = Executors.newFixedThreadPool(threadSize);
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, Object>> dataList) {
        List<List<ConsumerRecord<String, Object>>> lists = splitList(dataList);
        CountDownLatch latch = new CountDownLatch(lists.size());
        lists.forEach(consumerRecords -> {
            executorService.submit(() -> {
                Map<String, List<String>> topicTorcvDataList = new LinkedHashMap<>();

                consumerRecords.forEach(data -> {
                    try {
                        if (!topicTorcvDataList.containsKey(data.topic())) {
                            List<String> rcvDataList = new LinkedList<>();
                            rcvDataList.add(objectMapper.writeValueAsString(data.value()));
                            topicTorcvDataList.put(data.topic(), rcvDataList);
                        } else {
                            topicTorcvDataList.get(data.topic()).add(objectMapper.writeValueAsString(data.value()));
                        }
                    } catch (JsonProcessingException e) {
                        log.error("Failed to serialize incoming message", e);
                    }
                });

                for (Map.Entry<String, List<String>> entry : topicTorcvDataList.entrySet()) {
                    persistService.persist(entry.getKey(), entry.getValue());
                    if (!entry.getKey().equalsIgnoreCase(persistAuditKafkaTopic)) {
                        Map<String, Object> producerRecord = new HashMap<>();
                        producerRecord.put("topic", entry.getKey());
                        producerRecord.put("value", entry.getValue());
                        kafkaTemplate.send(auditGenerateKafkaTopic, producerRecord);
                    }
                }
                latch.countDown();
            });

        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> List<List<T>> splitList(List<T> dataList) {
        List<List<T>> subLists = new LinkedList<>();
        if (dataList.size() < 100) {
            subLists.add(dataList);
            return subLists;
        }
        int partitionSize = dataList.size() / threadSize;
        AtomicInteger lastIndex = new AtomicInteger();
        for (int i = 0; i < threadSize; i++) {
            int index = lastIndex.getAndAdd(partitionSize);
            if (i == threadSize - 1) {
                subLists.add(dataList.subList(index, dataList.size()));
            } else {
                subLists.add(dataList.subList(index, lastIndex.get() + 1));
            }
        }
        return subLists;
    }


}
