package com.bay.sessionanalyze.dao;

import com.bay.sessionanalyze.domain.Top10Category;

/**
 * top10品类DAO接口
 *
 * @author Administrator
 */
public interface ITop10CategoryDAO {
    void truncate();

    void insert(Top10Category category);

}
