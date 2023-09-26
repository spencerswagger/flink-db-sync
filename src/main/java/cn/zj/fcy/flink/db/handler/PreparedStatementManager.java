package cn.zj.fcy.flink.db.handler;

import cn.hutool.core.io.IoUtil;
import cn.zj.fcy.flink.db.util.JdbcUtils;
import lombok.ToString;
import org.apache.commons.dbcp2.BasicDataSource;

import java.io.Serializable;
import java.sql.PreparedStatement;

/**
 * 用于执行jdbc sql的处理器
 *
 * @author fcy
 */
@ToString
public class PreparedStatementManager implements Serializable {
    @ToString.Exclude
    private PreparedStatement preparedStatement;
    private final String databaseProperties;
    @ToString.Exclude
    private BasicDataSource dataSource;
    private final String sql;

    public PreparedStatementManager(String databaseProperties, String sql) {
        this.sql = sql;
        this.databaseProperties = databaseProperties;
    }

    public void prepareSql() {
        if (!JdbcUtils.isValid(dataSource)) {
            this.dataSource = JdbcUtils.getDatasource(databaseProperties);
        }
        JdbcUtils.checkValid(preparedStatement = JdbcUtils.preparedSql(dataSource, sql));
    }

    public void close() {
        if (JdbcUtils.isValid(dataSource)) {
            IoUtil.close(dataSource);
        }
    }

    public PreparedStatement get() {
        return get(false);
    }

    public PreparedStatement get(boolean newInstance) {
        // 当前SQL缓存已失效时重新加载
        if (!JdbcUtils.isValid(preparedStatement) || newInstance) {
            prepareSql();
        }
        return preparedStatement;
    }
}
