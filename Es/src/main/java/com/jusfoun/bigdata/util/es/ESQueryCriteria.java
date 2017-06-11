package com.jusfoun.bigdata.util.es;

import org.elasticsearch.search.sort.SortOrder;

/**
 * ES 查询条件
 * 
 * @author admin
 *
 */
public class ESQueryCriteria {
	
	private String index; // 查询的索引 ，这个字段是必须的 
	private String type; // 查询索引中的类型

	private String orderName;  //排序的字段 
	private SortOrder sortOrder= SortOrder.DESC;  //默认降序排序 
	
	private Integer curPage ; // 当前页
	private Integer startRow;// 当前页起始行
	private Integer pageSize = 20; // 每页多少行

	public void setCurPage(int curPage) {
		if (curPage < 1) {
			curPage = 1;
			startRow = 1 ; 
		} else {
			startRow = pageSize * (curPage - 1);
		}
		this.curPage = curPage;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Integer getCurPage() {
		return curPage;
	}

//	public void setCurPage(Integer curPage) {
//		this.curPage = curPage;
//	}

	public Integer getStartRow() {
		return startRow;
	}

	public void setStartRow(Integer startRow) {
		this.startRow = startRow;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public String getOrderName() {
		return orderName;
	}

	public void setOrderName(String orderName) {
		this.orderName = orderName;
	}

	public SortOrder getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(SortOrder sortOrder) {
		this.sortOrder = sortOrder;
	}

}
