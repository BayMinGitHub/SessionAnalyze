package com.bay.sessionanalyze.dao;


import com.bay.sessionanalyze.domain.AdProvinceTop3;

import java.util.List;

/**
 * 各省份top3热门广告DAO接口
 *
 * @author Administrator
 */
public interface IAdProvinceTop3DAO {
    void truncate();

    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);

}
