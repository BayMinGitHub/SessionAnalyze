package com.bay.sessionanalyze.dao.impl;

import com.bay.sessionanalyze.dao.ISessionDetailDAO;
import com.bay.sessionanalyze.domain.SessionDetail;
import com.bay.sessionanalyze.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * session明细DAO实现类
 * @author Administrator
 *
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()};  
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
	/**
	 * 批量插入session明细数据
	 * @param sessionDetails
	 */
	public void insertBatch(List<SessionDetail> sessionDetails) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";  
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(SessionDetail sessionDetail : sessionDetails) {
			Object[] params = new Object[]{sessionDetail.getTaskid(),
					sessionDetail.getUserid(),
					sessionDetail.getSessionid(),
					sessionDetail.getPageid(),
					sessionDetail.getActionTime(),
					sessionDetail.getSearchKeyword(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getOrderCategoryIds(),
					sessionDetail.getOrderProductIds(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds()};  
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
	}
	
}
