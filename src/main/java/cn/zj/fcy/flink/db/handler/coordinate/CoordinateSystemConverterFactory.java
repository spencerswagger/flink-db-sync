package cn.zj.fcy.flink.db.handler.coordinate;

import cn.hutool.core.util.EnumUtil;
import cn.hutool.setting.dialect.Props;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static cn.zj.fcy.flink.db.constant.ConfigConstant.COORDINATE_FROM;
import static cn.zj.fcy.flink.db.constant.ConfigConstant.COORDINATE_TO;

/**
 * 坐标系转换器工厂
 *
 * @author fcy
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoordinateSystemConverterFactory {
    /**
     * 获取坐标系转换器
     *
     * @param from 原坐标系
     * @param to   目标坐标系
     * @return 坐标系转换器
     */
    public static CoordinateSystemConverter get(CoordinateSystem from, CoordinateSystem to) {
        if (from == null || to == null || from == to) {
            log.error("坐标系转换配置错误，坐标系不能为空或者相同");
            return null;
        }
        switch (from) {
            case WGS84:
                switch (to) {
                    case BD09:
                    case GCJ02:
                    default:
                        break;
                }
            case GCJ02:
                switch (to) {
                    case WGS84:
                        return new Gcj02Wgs84Converter();
                    case BD09:
                    default:
                        break;
                }
            default:
                throw new UnsupportedOperationException("不支持的坐标系转换");
        }
    }
    public static CoordinateSystemConverter get(Props props) {
        if (props.containsKey(COORDINATE_FROM) && props.containsKey(COORDINATE_TO)) {
            CoordinateSystem from = EnumUtil.fromString(CoordinateSystem.class, props.getStr(COORDINATE_FROM));
            CoordinateSystem to = EnumUtil.fromString(CoordinateSystem.class, props.getStr(COORDINATE_TO));
            return get(from, to);
        }
        log.error("坐标系转换配置错误，缺少必要的配置项");
        return null;
    }
}
