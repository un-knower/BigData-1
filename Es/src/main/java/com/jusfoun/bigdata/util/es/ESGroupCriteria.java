package com.jusfoun.bigdata.util.es;

public class ESGroupCriteria {

	private String groupField ; // 根据那个字段来分组 ，查询条件必须有
	private Object fieldValue ; // 返回字段的val
	private Long fieldCount ;  //这个字段的值 有多少 
	
	public String getGroupField() {
		return groupField;
	}
	public void setGroupField(String groupField) {
		this.groupField = groupField;
	}
	 
	public Object getFieldValue() {
		return fieldValue;
	}
	public void setFieldValue(Object fieldValue) {
		this.fieldValue = fieldValue;
	}
	public Long getFieldCount() {
		return fieldCount;
	}
	public void setFieldCount(Long fieldCount) {
		this.fieldCount = fieldCount;
	}
	@Override
	public String toString() {
		 
		return this.getFieldValue()+"  "+this.getFieldCount() ;
	}
 
	
}
