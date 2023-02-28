package org.egov.infra.persist.service;

import com.github.zafarkhaja.semver.Version;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.egov.infra.persist.repository.PersistRepository;
import org.egov.infra.persist.repository.Sql2oBatchRepository;
import org.egov.infra.persist.repository.Sql2oRawRepository;
import org.egov.infra.persist.utils.Utils;
import org.egov.infra.persist.web.contract.JsonMap;
import org.egov.infra.persist.web.contract.Mapping;
import org.egov.infra.persist.web.contract.QueryMap;
import org.egov.infra.persist.web.contract.TopicMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class Sql2oPersistService {

    @Autowired
    private TopicMap topicMap;

    @Autowired
    private PersistRepository persistRepository;

    @Autowired
    private Sql2oBatchRepository sql2OBatchRepository;

    @Autowired
    private Sql2oRawRepository sql2oRawRepository;

    @Autowired
    private Utils utils;

    public void persist(String topic, String json) {

        Map<String, List<Mapping>> map = topicMap.getTopicMap();

        Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
        List<Mapping> applicableMappings = filterMappings(map.get(topic), document);
        log.info("{} applicable configs found!", applicableMappings.size());

        for (Mapping mapping : applicableMappings) {
            List<QueryMap> queryMaps = mapping.getQueryMaps();
            for (QueryMap queryMap : queryMaps) {
                String query = queryMap.getQuery();
                List<JsonMap> jsonMaps = queryMap.getJsonMaps();
                String basePath = queryMap.getBasePath();
                persistRepository.persist(query, jsonMaps, document, basePath);
            }
        }
    }

    public void persist(String topic, List<String> jsons) {

        Map<String, List<Mapping>> map = topicMap.getTopicMap();
        Map<Object, List<Mapping>> applicableMappings = new LinkedHashMap<>();

        for (String json : jsons) {
            Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
            applicableMappings.put(document, filterMappings(map.get(topic), document));
        }

        Map<String, List<Object[]>> qMap = new LinkedHashMap<>();
        applicableMappings.forEach((jsonObj, mappings) -> {
            for (Mapping mapping : mappings) {
                List<QueryMap> queryMaps = mapping.getQueryMaps();
                for (QueryMap queryMap : queryMaps) {
                    String query = queryMap.getQuery();
                    List<JsonMap> jsonMaps = queryMap.getJsonMaps();
                    String basePath = queryMap.getBasePath();
                    List<Object[]> rows = new LinkedList<>(persistRepository.getRows(jsonMaps, jsonObj, basePath));
                    qMap.computeIfAbsent(query, s -> new LinkedList<>()).addAll(rows);
                }
            }
        });
        qMap.forEach((s, objects) -> {
            sql2OBatchRepository.persist(s,objects);
        });
    }

    private List<Mapping> filterMappings(List<Mapping> mappings, Object json) {
        List<Mapping> filteredMaps = new ArrayList<>();
        String version = "";
        try {
            version = JsonPath.read(json, "$.RequestInfo.ver");
        } catch (PathNotFoundException ignore) {
        }
        Version semVer = utils.getSemVer(version);
        for (Mapping map : mappings) {
            if (semVer.satisfies(map.getVersion()))
                filteredMaps.add(map);
        }

        return filteredMaps;
    }

}