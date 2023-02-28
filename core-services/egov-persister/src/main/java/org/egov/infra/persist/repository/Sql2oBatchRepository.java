package org.egov.infra.persist.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2o;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class Sql2oBatchRepository {

    private Map<String, String> queryMap = new HashMap<>(); // regular to sql2o format query map
    private String[] paramNames = new String[100];

    private Sql2o sql2o;
    @Autowired
    private Sql2oCreator creator;
    @PostConstruct
    public void init() {
        //todo initialize sql2o here, with a datasource and connection pool
        this.sql2o = creator.getSql2o();
        for (int i = 0; i < paramNames.length; i++) {
            paramNames[i] = "p" + i;
        }
    }

    public void persist(String query, List<Object[]> rows) {

        if (rows.isEmpty()) return;

        String sql2oQueryText = getSql2oQueryText(query);

        try (Connection con = sql2o.beginTransaction()) {
            log.info("Executing sql2o query : " + sql2oQueryText);
            Query sql2oQuery = con.createQuery(sql2oQueryText);
            for (Object[] params : rows) {
                for (int i = 0; i < params.length; i++) {
                    sql2oQuery.addParameter(paramNames[i], params[i]);
                }
                sql2oQuery.addToBatch();
            }
            sql2oQuery.executeBatch();
            con.commit();
            log.info("Persisted {} row(s) to DB!", rows.size());
        } catch (Exception ex) {
            log.error("Failed to persist {} row(s) using query: {}", rows.size(), query, ex);
            throw ex;
        }
    }

    private String getSql2oQueryText(String query) {

        if (queryMap.containsKey(query)) return queryMap.get(query);

        String sql2oQuery = query;
        int i = 0;
        while (sql2oQuery.contains("?")) {
            sql2oQuery = sql2oQuery.replaceFirst("\\?", ":p" + i);
            i++;
        }
        queryMap.put(query, sql2oQuery);
        return sql2oQuery;
    }

    private String prepareSql(String query, List<Object[]> rows) {

        StringBuilder sb = new StringBuilder((query.length() + 100) * rows.size());

        sb.append("begin;");
        sb.append("commit;");

        return sb.toString();
    }

}
