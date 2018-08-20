package com.bay.sessionanalyze.dao;

import com.bay.sessionanalyze.domain.Top10Session;

/**
 * top10活跃session的DAO接口
 *
 * @author Administrator
 */
public interface ITop10SessionDAO {
    void truncate();

    void insert(Top10Session top10Session);

}
