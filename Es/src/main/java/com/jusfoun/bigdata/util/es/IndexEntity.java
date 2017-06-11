package com.jusfoun.bigdata.util.es;

public class IndexEntity {
	private String index ; 
	private String type ; 
	private String id  ; 
	private String parentId  ; //当需要父子结构数据时候需要给这个值s
	
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
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getParentId() {
		return parentId;
	}
	public void setParentId(String parentId) {
		this.parentId = parentId;
	} 
	
	
}
