package com.jusfoun.bigdata.util;

import java.util.Date;

public class User {
	private Long id ;
	private String name ; 
	private Integer age ;
	private Date tcreateDate ; 
	private Long tcreateDateLong ; 
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public Date getTcreateDate() {
		return tcreateDate;
	}
	public void setTcreateDate(Date tcreateDate) {
		this.tcreateDate = tcreateDate;
	}
	public Long getTcreateDateLong() {
		return tcreateDateLong;
	}
	public void setTcreateDateLong(Long tcreateDateLong) {
		this.tcreateDateLong = tcreateDateLong;
	}
	 
	
	
}
