package cn.zj.fcy.flink.db;

import cn.hutool.core.map.MapUtil;
import cn.hutool.setting.Setting;
import cn.hutool.setting.dialect.Props;
import cn.zj.fcy.flink.db.sink.SingleJdbcSink;
import cn.zj.fcy.flink.db.source.RollingJdbcSource;
import cn.zj.fcy.flink.db.transformer.CoordinateTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;

import static cn.zj.fcy.flink.db.constant.ConfigConstant.*;

/**
 * 创建数据同步任务
 *
 * @author fcy
 */
@Slf4j
public class DataSyncJob {
    public static void main(String[] args) throws Exception {
        if (StringUtils.isBlank(args[0])) {
            throw new FlinkRuntimeException("配置文件路径不能为空");
        }
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取配置文件
        final Setting setting = new Setting(args[0]);
        final Props sourceSetting = setting.getProps(SOURCE_NAME);
        final Props sinkSetting = setting.getProps(SINK_NAME);
        final Props transformSetting = setting.getProps(TRANSFORM_NAME);
        final String jobName = setting.getStr(NAME, args[0]);
        // 设置源
        SingleOutputStreamOperator<Map<String, Object>> source = env.addSource(new RollingJdbcSource(sourceSetting))
                                                                    .name(sourceSetting.getStr(NAME,
                                                                        jobName + SOURCE_SUFFIX));
        if (MapUtil.isNotEmpty(transformSetting)) {
            source = source.map(new CoordinateTransformer(transformSetting))
                           .name(transformSetting.getStr(NAME, jobName + TRANSFORM_SUFFIX));

        }
        // 设置sink
        source.addSink(new SingleJdbcSink(sinkSetting))
              .name(sinkSetting.getStr(NAME, jobName + SINK_SUFFIX));

        // 执行
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(30, 10000));
        env.execute(jobName);
    }
}
