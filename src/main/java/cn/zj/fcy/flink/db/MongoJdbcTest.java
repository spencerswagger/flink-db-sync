package cn.zj.fcy.flink.db;

import cn.zj.fcy.flink.db.util.JdbcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;

@Slf4j
public class MongoJdbcTest {
    public static void main(String[] args) throws SQLException {
        BasicDataSource dataSource = JdbcUtils.getDatasource("examples/db-mongo-example.properties");
        String sql = "db.getCollection(\"test\").find({datetime:{$gt:\"?\"}}).limit(21)";
        log.info("开始准备sql");
        PreparedStatement preparedStatement = JdbcUtils.preparedSql(dataSource, sql);

        log.info("设置变量");
                preparedStatement.setObject(1, "2022-01-01 00:00:00");
        execute(preparedStatement);

        log.info("更改变量");
                preparedStatement.setObject(1, "2022-01-01 10:00:00");
                execute(preparedStatement);

        log.info("关闭连接");
        dataSource.close();
    }

    private static void execute(PreparedStatement preparedStatement) throws SQLException {

        log.info("执行sql:{}", preparedStatement);
        ResultSet rs = preparedStatement.executeQuery();
        log.info("获取结果");
        // 获取字段信息
        // TODO 缓存字段信息
        ResultSetMetaData metaData = rs.getMetaData();
        // 获取记录到map中
        while (rs.next()) {
            LinkedHashMap<String, Object> record = new LinkedHashMap<>(metaData.getColumnCount());
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String name = metaData.getColumnName(i);
                record.put(name, rs.getObject(name));
            }
            log.info("记录:{}", record.entrySet());
        }
        rs.close();
    }
}