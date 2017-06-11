package com.chakao.hbase.model;


import com.chakao.hbase.ORMHBaseColumn;
import com.chakao.hbase.ORMHBaseTable;

@ORMHBaseTable(tableName = "mobileprofile")
public class MUser {
	@ORMHBaseColumn(family = "rowkey", qualifier = "rowkey")
	private String id;
	@ORMHBaseColumn(family = "f1", qualifier = "w")
	private String content;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "MUser [id=" + id + ", content=" + content + "]";
	}

}
