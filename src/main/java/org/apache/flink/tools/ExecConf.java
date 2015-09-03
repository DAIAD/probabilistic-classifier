package org.apache.flink.tools;

import java.io.*;

public class ExecConf implements Serializable {

	private static final long serialVersionUID = 1L;

	private int knn = 15;
	private int shift = 2;
	private int nr = 27296;
	private int ns = 244169;
	private int dimension = 12;
	private int noOfClasses = 5;
	private double epsilon = 0.003;
	private int numOfPartition = 8;
	private boolean hilbertOrZ = false;
	private int[] scale = { 8, 50, 100, 90, 7, 90, 90, 100, 50, 100, 100, 120 };
	private int[][] shiftvectors;
	private String hdfsPath = "the-hdfs-path";
	private String localPath = "the-local-path";
	private String sourcesPath = "the-sources-path";

	public ExecConf() {
	}

	public int getKnn() {
		return knn;
	}

	public void setKnn(int knn) {
		this.knn = knn;
	}

	public int getShift() {
		return shift;
	}

	public void setShift(int shift) {
		this.shift = shift;
	}

	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public int getNs() {
		return ns;
	}

	public void setNs(int ns) {
		this.ns = ns;
	}

	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

	public int getNoOfClasses() {
		return noOfClasses;
	}

	public void setNoOfClasses(int noOfClasses) {
		this.noOfClasses = noOfClasses;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	public int getNumOfPartition() {
		return numOfPartition;
	}

	public void setNumOfPartition(int numOfPartition) {
		this.numOfPartition = numOfPartition;
	}

	public boolean isHilbertOrZ() {
		return hilbertOrZ;
	}

	public void setHilbertOrZ(boolean hilbertOrZ) {
		this.hilbertOrZ = hilbertOrZ;
	}

	public int[] getScale() {
		return scale;
	}

	public void setScale(int[] scale) {
		this.scale = scale;
	}

	public int[][] getShiftvectors() {
		return shiftvectors;
	}

	public void setShiftvectors(int[][] shiftvectors) {
		this.shiftvectors = shiftvectors;
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}
	
	public String getLocalPath() {
		return localPath;
	}
	
	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public String getSourcesPath() {
		return sourcesPath;
	}

	public void setSourcesPath(String sourcesPath) {
		this.sourcesPath = sourcesPath;
	}
}
