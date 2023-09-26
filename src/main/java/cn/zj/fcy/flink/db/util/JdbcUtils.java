package cn.zj.fcy.flink.db.util;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.setting.dialect.Props;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * jdbc工具类
 * 基于dbcp2实现
 * @author fcy
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class JdbcUtils {
    /**
     * 重试次数
     */
    public static int MAX_RETRY = 3;
    /**
     * 重试等待秒数
     */
    public static int RETRY_WAIT = 10;
    /**
     * 数据库连接超时时间
     */
    public static int CONNECTION_TIMEOUT = 1000;

    public static BasicDataSource getDatasource(String filePath) {
        return getDatasource(new Props(filePath));
    }

    public static BasicDataSource getDatasource(Props properties) {
        try {
            log.info("正在创建数据源:{}", properties.getStr("url"));
            BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
            checkValid(dataSource);
            log.info("创建数据源成功");
            return dataSource;
        } catch (Exception e) {
            log.error("创建数据源失败", e);
            return null;
        }
    }

    public static Connection getConnection(BasicDataSource dataSource) {
        checkValid(dataSource);
        return getConnection(dataSource, 1);
    }

    private static Connection getConnection(BasicDataSource dataSource, int retry) {
        checkValid(dataSource);
        if (retry > MAX_RETRY) {
            log.error("获取数据库连接达到最大次数{}", MAX_RETRY);
            return null;
        }
        try {
            log.info("正在获取数据库连接：{}", dataSource.getUrl());
            Connection connection = dataSource.getConnection();
            log.info("数据库连接成功：{}", connection);
            return connection;
        } catch (Exception e) {
            log.error("获取数据库连接失败:{}", dataSource.getUrl());
            log.error("获取数据库连接失败原因:{}", e.getMessage(), e);
            log.info("等待{}秒后将尝试再次获取数据库连接", RETRY_WAIT);
            ThreadUtil.sleep(retry * 1000L);
            return getConnection(dataSource, retry + 1);
        }
    }

    public static PreparedStatement preparedSql(Connection connection, String sql) {
        try {
            if (isValid(connection)) {
                log.info("即将初始化sql语句{}", sql);
                return connection.prepareStatement(sql);
            } else {
                log.error("数据库连接为空或连接已关闭");
            }
        } catch (SQLException e) {
            log.error("预加载SQL失败", e);
        }
        return null;
    }

    public static PreparedStatement preparedSql(BasicDataSource dataSource, String sql) {
        return preparedSql(dataSource, sql, 1);
    }

    private static PreparedStatement preparedSql(BasicDataSource dataSource, String sql, int retry) {
        if (retry > MAX_RETRY) {
            log.error("加载SQL达到最大次数{}", MAX_RETRY);
            return null;
        }
        // 从数据源获取连接
        Connection connection = getConnection(dataSource);
        // 使用当前连接预加载SQL
        PreparedStatement preparedStatement = preparedSql(connection, sql);
        if (!isValid(preparedStatement)) {
            log.info("等待{}秒后将尝试再次加载SQL", RETRY_WAIT);
            ThreadUtil.sleep(retry * 1000L);
            return preparedSql(dataSource, sql, retry + 1);
        }
        return preparedStatement;
    }

    public static boolean isValid(BasicDataSource dataSource) {
        return dataSource != null && !dataSource.isClosed();
    }

    public static boolean isValid(Connection connection) {
        try {
            return connection != null && connection.isValid(CONNECTION_TIMEOUT);
        } catch (SQLException e) {
            return false;
        }
    }

    public static boolean isValid(PreparedStatement preparedStatement) {
        try {
            return preparedStatement != null && !preparedStatement.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    public static void checkValid(BasicDataSource dataSource) {
        Assert.isTrue(isValid(dataSource), "数据源已失效");
    }

    public static void checkValid(PreparedStatement preparedStatement) {
        Assert.isTrue(isValid(preparedStatement), "SQL已失效");
    }
}
