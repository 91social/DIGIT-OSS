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
public class Sql2oRawRepository {

    private Map<String, String> queryMap = new HashMap<>(); // regular to sql2o format query map
    private String[] paramNames = new String[100];

    private Sql2o sql2o;

    @Autowired
    private Sql2oCreator creator;

    @PostConstruct
    public void init() {
        //todo initialize sql2o here, with a datasource and connection pool
        this.sql2o = creator.getSql2o();
    }

    public void persist(String query, List<Object[]> rows) {

        if (rows.isEmpty()) return;

        String sql2oQueryText = prepareSql(query, rows);

        try (Connection con = sql2o.open()) {
            log.info("Executing raw sql2o query : " + sql2oQueryText);
            Query sql2oQuery = con.createQuery(sql2oQueryText);
            sql2oQuery.executeUpdate();
            log.info("Persisted {} row(s) to DB!", rows.size());
        } catch (Exception ex) {
            log.error("Failed to persist {} row(s) using query: {}", rows.size(), query, ex);
            throw ex;
        }
    }

    private String prepareSql(String query, List<Object[]> rows) {

        StringBuilder sb = new StringBuilder((query.length() + 100) * rows.size());

        sb.append("begin;\n");

        for (Object[] params : rows) {

            int i = 0;
            for (int j = 0; j < query.length(); j++) {
                char c = query.charAt(0);
                if (c != '?') {
                    sb.append(c);
                    continue;
                }
                Object value = params[i];
                String paramValue = "''";
                if (value != null) {
                    if (value instanceof Number) {
                        paramValue = value.toString();
                    } else {
                        paramValue = "'" + value + "'";
                    }
                }
                i++;
                sb.append(paramValue);
            }
            sb.append('\n');
        }

        sb.append("commit;");

        return sb.toString();
    }

}
