package org.apache.flink.tools;

import java.io.Serializable;

public class KnnRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private String rid;
	private float dist;

	public KnnRecord(String rid, float dist) {
		this.rid = rid;
		this.dist = dist;
	}

	public float getDist() {
		return this.dist;
	}

	public String getRid() {
		return this.rid;
	}

	public String toString() {
		return this.rid + " " + Float.toString(this.dist);
	}
}
