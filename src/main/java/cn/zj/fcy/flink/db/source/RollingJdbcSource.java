package cn.zj.fcy.flink.db.source;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.setting.dialect.Props;
import cn.zj.fcy.flink.db.handler.PreparedStatementManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import static cn.zj.fcy.flink.db.constant.ConfigConstant.*;

/**
 * 滚动查询数据源
 * jdbc实现
 *
 * @author fcy
 */
@Slf4j
public class RollingJdbcSource extends RichSourceFunction<Map<String, Object>> {
    private boolean canceled = false;
    private final String cursorKey;
    private Object cursor;
    private final PreparedStatementManager preparedStatementManager;
    private int fallbackCount = 0;

    public RollingJdbcSource(Props props) {
        this(props.getStr(DB_NAME), props.getStr(SQL), props.get(SOURCE_START_CURSOR), props.getStr(SOURCE_CURSOR_KEY));
    }

    public RollingJdbcSource(String databaseProperties, String sql, Object startCursor, String cursorKey) {
        this.cursor = startCursor;
        this.cursorKey = cursorKey;
        this.preparedStatementManager = new PreparedStatementManager(databaseProperties, sql);
    }

    @Override
    public void run(SourceContext<Map<String, Object>> sourceContext) throws Exception {
        PreparedStatement preparedStatement = preparedStatementManager.get();
        while (!canceled) {
            // 设置当前同步点位
            // 注意同步点位字段目前只能在第一个变量
            preparedStatement.setObject(1, cursor);
            // 记录当前同步点位
            Object startCursor = this.cursor;
            if (log.isDebugEnabled()) {
                log.debug("正在执行sql:{}", preparedStatement);
            }
            // 执行查询
            ResultSet rs = null;
            try {
                rs = preparedStatement.executeQuery();
            } catch (SQLException e) {
                log.error("执行sql失败，即将重试", e);
                IoUtil.close(preparedStatement);
                fallbackCount++;
                if (fallbackCount <= 3) {
                    run(sourceContext);
                    fallbackCount = 0;
                } else {
                    throw new FlinkRuntimeException("超出最大尝试次数");
                }
                return;
            }
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
                // 传入context
                sourceContext.collect(record);
                // 移动同步点位
                this.cursor = record.get(cursorKey);
            }
            if (ObjectUtil.notEqual(this.cursor, startCursor)) {
                log.info("同步点位更新至:{}", cursor);
            }
            // 等待下一次执行
            // TODO 应判断是否取完 再判断是否需要等待
            ThreadUtil.sleep(3000);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        preparedStatementManager.close();
    }

    @Override
    public void cancel() {
        log.info("任务已被取消");
        this.canceled = true;
    }
}
