package cn.zj.fcy.flink.db.constant;

import cn.hutool.core.collection.ListUtil;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 配置文件key定义
 *
 * @author fcy
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConfigConstant {
    /**
     * 数据库配置文件路径
     *
     * @see cn.hutool.setting.dialect.Props
     */
    public static final String DB_NAME = "db";
    /**
     * 执行的sql语句模版
     *
     * @see java.sql.PreparedStatement
     */
    public static final String SQL = "sql";
    /**
     * 用于筛选更新数据项的字段名
     */
    public static final String SOURCE_CURSOR_KEY = "cursorKey";
    /**
     * 数据同步初始点位
     */
    public static final String SOURCE_START_CURSOR = "startCursor";
    /**
     * 数据源分组名称
     */
    public static final String SOURCE_NAME = "source";
    /**
     * 目标库分组名称
     */
    public static final String SINK_NAME = "sink";
    /**
     * 转换器分组名称
     */
    public static final String TRANSFORM_NAME = "transform";
    /**
     * 转换器类型
     */
    public static final String TRANSFORM_TYPE = "type";
    /**
     * 坐标系转换器类型名称
     */
    public static final String COORDINATE_TRANSFORMER_NAME = "coordinate-convert";
    /**
     * 经纬度原坐标系
     */
    public static final String COORDINATE_FROM = "from";
    /**
     * 经纬度目标坐标系
     */
    public static final String COORDINATE_TO = "to";
    /**
     * 经度的字段名
     */
    public static final String COORDINATE_LON = "lon";
    /**
     * 纬度的字段名
     */
    public static final String COORDINATE_LAT = "lat";
    /**
     * 可能的经度字段名
     */
    public static final List<String> DEFAULT_COORDINATE_LON = ListUtil.of("lon", "longitude", "x", "lng");
    /**
     * 可能的纬度字段名
     */
    public static final List<String> DEFAULT_COORDINATE_LAT = ListUtil.of("lat", "latitude", "y");
    /**
     * 任务名、组件名等名称
     */
    public static final String NAME = "name";
    /**
     * 数据源默认后缀
     */
    public static final String SOURCE_SUFFIX = "-Source";
    /**
     * 目标库默认后缀
     */
    public static final String SINK_SUFFIX = "-Sink";
    /**
     * 转换器默认后缀
     */
    public static final String TRANSFORM_SUFFIX = "-Transformer";
}
