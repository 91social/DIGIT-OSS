package org.egov.infra.persist.repository;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.sql2o.Sql2o;

@Service
public class Sql2oCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sql2oCreator.class);

    private Sql2o sql2o;
    private DatabaseConfig config;

    public Sql2oCreator(DatabaseConfig config) {
        this.config = config;
        HikariDataSource dataSource = new HikariDataSource(createHikariConfig());
        this.sql2o = new Sql2o(dataSource);
    }

    private HikariConfig createHikariConfig() {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.getUrl());
        hc.setUsername(config.getUser());
        hc.setPassword(config.getPassword());
        hc.addDataSourceProperty("cachePrepStmts", "true");
        hc.addDataSourceProperty("prepStmtCacheSize", "250");
        hc.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hc.setMaximumPoolSize(config.getMaxPoolSize());
        return hc;
    }

    public DatabaseConfig getConfig() {
        return config;
    }

    public Sql2o getSql2o() {
        return sql2o;
    }
}
