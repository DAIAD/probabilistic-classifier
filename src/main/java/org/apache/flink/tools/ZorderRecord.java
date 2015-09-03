package org.apache.flink.tools;

import java.io.*;

public class ZorderRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private String first;
	private String second;
	private Integer third;
	private Integer fourth;
	private Integer demandClass;

	public ZorderRecord() {
	}

	public ZorderRecord(String first, String id, Integer third,
			Integer fourth, Integer demandClass) {
		this.first = first;
		this.second = id;
		this.third = third;
		this.fourth = fourth;
		this.demandClass = demandClass;
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	public Integer getThird() {
		return third;
	}

	public Integer getdemandClass() {
		return demandClass;
	}

	public void setThird(Integer third) {
		this.third = third;
	}

	public Integer getFourth() {
		return fourth;
	}

	public void setFourth(Integer fourth) {
		this.fourth = fourth;
	}

	public void setdemandClass(Integer demandClass) {
		this.demandClass = demandClass;
	}

	public String toString() {
		return first + " " + second.toString() + " " + third.toString() + " "
				+ fourth.toString() + " " + demandClass.toString();
	}
}
