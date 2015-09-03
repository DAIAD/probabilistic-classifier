package org.apache.flink.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Random;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

public class Functions {

	public static void createDatasetsRandom(String alicanteFilename,
			ExecConf conf) throws IOException {

		final InputStream in = new BufferedInputStream(new FileInputStream(
				alicanteFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		// Throw first line
		String line = null;
		line = br.readLine();
		Random r = new Random();

		Path rPt = new Path(conf.getHdfsPath() + "datasets/R");
		Path sPt = new Path(conf.getHdfsPath() + "datasets/S");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(sPt, true)));

		while ((line = br.readLine()) != null) {
			if (r.nextDouble() < 0.1) {
				rBw.write(line + "\n");
			} else {
				sBw.write(line + "\n");
			}
		}

		rBw.close();
		sBw.close();
		br.close();
	}

	public static void createDatasetsCrossValid(
			String alicanteFilename, ExecConf conf, int fold,
			int requestedPartition) throws IOException {

		InputStream in = new BufferedInputStream(new FileInputStream(
				alicanteFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		LineNumberReader lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		int datasetSize = (lnr.getLineNumber() + 1);
		int partitionSize = datasetSize / fold;

		// Throw first line
		in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		br = new BufferedReader(new InputStreamReader(in));
		String line = null;
		line = br.readLine();

		Path rPt = new Path(conf.getHdfsPath() + "datasets/R");
		Path sPt = new Path(conf.getHdfsPath() + "datasets/S");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(sPt, true)));

		int counter = 0;
		int entryPoint = requestedPartition * partitionSize;
		int exitPoint = (requestedPartition + 1) * partitionSize;
		while ((line = br.readLine()) != null) {
			if ((counter >= entryPoint) && (counter < exitPoint)) {
				rBw.write(line + "\n");
				counter++;
				continue;
			}
			sBw.write(line + "\n");
			counter++;
		}

		lnr.close();
		rBw.close();
		sBw.close();
		br.close();

	}

	// Method that generates the random shift vectors and stores them to file
	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static void genRandomShiftVectors(String filename, int dimension,
			int shift) throws IOException {

		Random r = new Random();
		int[][] shiftvectors = new int[shift][dimension];

		// Generate random shift vectors
		for (int i = 0; i < shift; i++) {
			shiftvectors[i] = createShift(dimension, r, true);
		}

		File vectorFile = new File(filename);
		if (!vectorFile.exists()) {
			vectorFile.createNewFile();
		}

		OutputStream outputStream = new FileOutputStream(filename);
		Writer writer = new OutputStreamWriter(outputStream);

		for (int j = 0; j < shift; j++) {
			String shiftVector = "";
			for (int k = 0; k < dimension; k++)
				shiftVector += Integer.toString(shiftvectors[j][k]) + " ";
			writer.write(shiftVector + "\n");
		}
		writer.close();
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static String createExtra(int num) {
		if (num < 1)
			return "";

		char[] extra = new char[num];
		for (int i = 0; i < num; i++)
			extra[i] = '0';
		return (new String(extra));
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static int[] createShift(int dimension, Random rin, boolean shift) {
		Random r = rin;
		int[] rv = new int[dimension]; // random vector

		if (shift) {
			for (int i = 0; i < dimension; i++) {
				rv[i] = ((int) Math.abs(r.nextInt(10001)));
			}
		} else {
			for (int i = 0; i < dimension; i++)
				rv[i] = 0;
		}
		return rv;
	}

	public static double calculateAccuracy(ExecConf conf, int num)
			throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "datasets/R");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		HashMap<String, Float> real = new HashMap<String, Float>();

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getDimension()+1]));
		}

		int i = 1;
		String lineResult;
		BufferedReader resultBr = null;
		int total_correct = 0;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "FinalKNNresults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(
						new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				break;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = 0;
				try {
					k = partsResult[0].indexOf(":");
					if (Float.parseFloat(partsResult[3]) ==
							real.get(partsResult[0].substring(0, k))) {
						total_correct++;
					}
				} catch (Exception e) {
					System.out.println("Error in item " + 
							real.get(partsResult[0].substring(0, k)));
				}
			}

			i++;
			resultBr.close();
		}

		resultBr.close();
		rBr.close();
		return 100.0 * total_correct / real.size();
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public static int getEstimatorIndex(int i, int size, double sampleRate,
			int numOfPartition) {
		double iquantile = (i * 1.0 / numOfPartition);
		int orgRank = (int) Math.ceil((iquantile * size));
		int estRank = 0;

		int val1 = (int) Math.floor(orgRank * sampleRate);
		int val2 = (int) Math.ceil(orgRank * sampleRate);

		int est1 = (int) (val1 * (1 / sampleRate));
		int est2 = (int) (val2 * (1 / sampleRate));

		int dist1 = (int) Math.abs(est1 - orgRank);
		int dist2 = (int) Math.abs(est2 - orgRank);

		if (dist1 < dist2)
			estRank = val1;
		else
			estRank = val2;

		return estRank;
	}
}