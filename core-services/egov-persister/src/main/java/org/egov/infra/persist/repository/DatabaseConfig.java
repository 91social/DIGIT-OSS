package org.egov.infra.persist.repository;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.sql.Connection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Configuration
public class DatabaseConfig {

    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.username}")
    private String user;
    @Value("${spring.datasource.password}")
    private String password;
    @Value("${spring.datasource.driver-class-name}")
    private String driverClass;
    private List<String> sqlFiles;
    private int minPoolSize;
    private int maxPoolSize;

    private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;

    private Map<String, String> columnMappings = Collections.emptyMap();

    public DatabaseConfig(){
        sqlFiles=new LinkedList<>();
        minPoolSize=2;
        maxPoolSize=30;
    }

    public String getUrl() {
        return url;
    }

    public DatabaseConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getUser() {
        return user;
    }

    public DatabaseConfig setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public DatabaseConfig setDriverClass(String driverClass) {
        this.driverClass = driverClass;
        return this;
    }

    public List<String> getSqlFiles() {
        return sqlFiles;
    }

    public DatabaseConfig setSqlFiles(List<String> sqlFiles) {
        this.sqlFiles = sqlFiles;
        return this;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public DatabaseConfig setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    public Map<String, String> getColumnMappings() {
        return columnMappings;
    }

    public DatabaseConfig setColumnMappings(Map<String, String> columnMappings) {
        this.columnMappings = columnMappings;
        return this;
    }

    public DatabaseConfig setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
        return this;
    }

    public int getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public String toString() {
        return "DatabaseConfig{" +
                "url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", driverClass='" + driverClass + '\'' +
                ", sqlFiles=" + sqlFiles +
                ", minPoolSize=" + minPoolSize +
                ", maxPoolSize=" + maxPoolSize +
                ", columnMappings=" + columnMappings +
                '}';
    }
}
