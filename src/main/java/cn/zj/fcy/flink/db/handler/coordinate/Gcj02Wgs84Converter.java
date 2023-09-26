package cn.zj.fcy.flink.db.handler.coordinate;

import cn.hutool.core.util.CoordinateUtil;

/**
 * gcj02转wgs84 经纬度转换器
 *
 * @author fcy
 */
public class Gcj02Wgs84Converter implements CoordinateSystemConverter {

    @Override
    public CoordinateUtil.Coordinate convert(CoordinateUtil.Coordinate coordinate) {
        return CoordinateUtil.gcj02ToWgs84(coordinate.getLng(), coordinate.getLat());
    }

}
