package cn.zj.fcy.flink.db.sink;

import cn.hutool.core.io.IoUtil;
import cn.hutool.setting.dialect.Props;
import cn.zj.fcy.flink.db.handler.PreparedStatementManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import static cn.zj.fcy.flink.db.constant.ConfigConstant.DB_NAME;
import static cn.zj.fcy.flink.db.constant.ConfigConstant.SQL;

/**
 * 单行数据写入jdbc
 *
 * @author fcy
 */
@Slf4j
public class SingleJdbcSink extends RichSinkFunction<Map<String, Object>> {

    private final PreparedStatementManager preparedStatementManager;
    private int fallbackCount = 0;

    public SingleJdbcSink(Props props) {
        this(props.getStr(DB_NAME), props.getStr(SQL));
    }

    public SingleJdbcSink(String databaseProperties, String sql) {
        this.preparedStatementManager = new PreparedStatementManager(databaseProperties, sql);
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param record
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Map<String, Object> record, Context context) throws SQLException {
        PreparedStatement preparedStatement = preparedStatementManager.get();
        // 获取参数总数
        int paramCount = preparedStatement.getParameterMetaData()
                                          .getParameterCount();
        // 遍历该条记录的所有值
        Iterator<Object> values = record.values()
                                        .iterator();
        int paramIndex = 0;
        while (values.hasNext()) {
            paramIndex++;
            if (paramIndex <= paramCount) {
                // 未超出变量需求数量 则添加到sql中
                preparedStatement.setObject(paramIndex, values.next());
            }
            // 若超出变量需求数量 则忽略
        }
        if (paramIndex < paramCount) {
            // 若处理完记录仍缺少变量 则抛出异常
            // TODO 可尝试填充null或default
            log.error("sink时参数数量{}少于变量数量{}", paramIndex, paramCount);
            throw new IllegalArgumentException("参数数量少于变量数量");
        }
        if (log.isDebugEnabled()) {
            log.debug("正在执行sql:{}", preparedStatement);
        }
        try {
            preparedStatement.execute();
        } catch (SQLException e) {
            log.error("执行sql失败，即将重试", e);
            IoUtil.close(preparedStatement);
            fallbackCount++;
            if (fallbackCount <= 3) {
                invoke(record, context);
                fallbackCount = 0;
            } else {
                throw new FlinkRuntimeException("超出最大尝试次数");
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.preparedStatementManager.close();
    }
}
