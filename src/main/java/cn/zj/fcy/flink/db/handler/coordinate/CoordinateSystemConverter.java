package cn.zj.fcy.flink.db.handler.coordinate;

import cn.hutool.core.util.CoordinateUtil;

import java.io.Serializable;

/**
 * 经纬度转换器
 *
 * @author fcy
 */
public interface CoordinateSystemConverter extends Serializable {
    /**
     * 经纬度转换
     *
     * @param coordinate 纬度
     * @return 转换后的经纬度
     */
    CoordinateUtil.Coordinate convert(CoordinateUtil.Coordinate coordinate);
}
