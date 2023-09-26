package cn.zj.fcy.flink.db.transformer;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.CoordinateUtil;
import cn.hutool.setting.dialect.Props;
import cn.zj.fcy.flink.db.handler.coordinate.CoordinateSystemConverter;
import cn.zj.fcy.flink.db.handler.coordinate.CoordinateSystemConverterFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;
import java.util.Map;

import static cn.zj.fcy.flink.db.constant.ConfigConstant.*;

@Slf4j
public class CoordinateTransformer implements MapFunction<Map<String, Object>, Map<String, Object>> {

    private String longitudeKey;
    private String latitudeKey;
    private CoordinateSystemConverter converter;

    public CoordinateTransformer(String longitudeKey, String latitudeKey,
        CoordinateSystemConverter converter) {
        this.longitudeKey = longitudeKey;
        this.latitudeKey = latitudeKey;
        this.converter = converter;
        log.info("初始化坐标转换器，经度字段：{}，纬度字段：{}，坐标转换器：{}", longitudeKey, latitudeKey, converter);
    }

    public CoordinateTransformer(Props props) {
        this(props.getStr(COORDINATE_LON), props.getStr(COORDINATE_LAT), CoordinateSystemConverterFactory.get(props));
        log.info("初始化坐标转换器，{}", props);
    }

    @Override
    public Map<String, Object> map(Map<String, Object> origin) {
        // 获取数据记录中的经纬度
        Double longitude = Convert.toDouble(origin.get(getCoordinateKey(origin, longitudeKey, DEFAULT_COORDINATE_LON)));
        Double latitude = Convert.toDouble(origin.get(getCoordinateKey(origin, latitudeKey, DEFAULT_COORDINATE_LAT)));
        // 任意经纬度为空则跳过
        if (longitude == null || latitude == null) {
            log.warn("经纬度字段值为空，跳过数据，数据：{}", origin);
            return origin;
        }
        //转换经纬度
        CoordinateUtil.Coordinate result = converter.convert(new CoordinateUtil.Coordinate(longitude, latitude));
        origin.put(longitudeKey, result.getLng());
        origin.put(latitudeKey, result.getLat());
        return origin;
    }

    /**
     * 自动获取经纬度字段值
     *
     * @param origin      数据列
     * @param key         用户提供的经纬度字段名
     * @param defaultKeys 默认经纬度字段名列表
     * @return 经纬度字段名
     */
    private String getCoordinateKey(Map<String, Object> origin, String key, List<String> defaultKeys) {
        String correctKey = key;
        if (StringUtils.isBlank(key)) {
            for (String defaultKey : defaultKeys) {
                if (origin.containsKey(defaultKey)) {
                    correctKey = defaultKey;
                    break;
                }
            }
        }
        return correctKey;
    }
}
