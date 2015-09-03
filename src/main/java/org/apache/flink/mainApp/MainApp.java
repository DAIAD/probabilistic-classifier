package org.apache.flink.mainApp;

import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.BufferedReader;

import org.apache.commons.io.FileUtils;

import java.io.File;

import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.ZorderRecord;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tools.Functions;
import org.apache.flink.transformations.MapPhase1;
import org.apache.flink.transformations.MapPhase2;
import org.apache.flink.transformations.ReducePhase1;
import org.apache.flink.transformations.ReducePhase2;
import org.apache.flink.transformations.ReducePhase3;

public class MainApp {

	public static void main(String[] args) throws Exception {

		ExecConf conf = new ExecConf();
		boolean isCrossValid = false;
		int folds = 10;

		// Read arguments and configure
		if (args.length > 0) {
			try {
				conf.setKnn(Integer.parseInt(args[0]));
				conf.setShift(Integer.parseInt(args[1]));
				conf.setDimension(Integer.parseInt(args[2]));
				conf.setNoOfClasses(Integer.parseInt(args[3]));
				conf.setEpsilon(Double.parseDouble(args[4]));
				conf.setNumOfPartition(Integer.parseInt(args[5]));
				conf.setHilbertOrZ(Boolean.parseBoolean(args[6]));
				// millions = Integer.parseInt(args[7]);
			} catch (Exception e) {
				System.out.println("Error! Please check arguments.");
				System.exit(0);
			}
		}

		// Generate and read random shift vectors
		Functions.genRandomShiftVectors(conf.getHdfsPath() + "RandomShiftVectors",
										conf.getDimension(), conf.getShift());
		int[][] shiftvectors = new int[conf.getShift()][conf.getDimension()];
		Path pt = new Path(conf.getHdfsPath() + "RandomShiftVectors");
		FileSystem fs = pt.getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		int j = 0;
		while (true) {
			String l = br.readLine();
			if ((l == null) || (j > conf.getShift() - 1))
				break;
			String[] p = l.split(" ");
			for (int i = 0; i < conf.getDimension(); i++)
				shiftvectors[j][i] = Integer.valueOf(p[i]);
			j++;
		}
		br.close();
		conf.setShiftvectors(shiftvectors);

		if (!isCrossValid) folds = 1;
		double totalPercentage = 0.0;
		for (int i = 0; i < folds; i++) {

			File S_local = new File(conf.getLocalPath() + "S_local/");
			FileUtils.cleanDirectory(S_local);

			Path fpt = new Path(conf.getHdfsPath() + "FinalKNNresults");
			fs = fpt.getFileSystem();
			fs.delete(fpt, true);

			if (isCrossValid) {
				Functions.createDatasetsCrossValid(conf.getSourcesPath() + 
						"input_dataset", conf, folds, i);
			} else {
				Functions.createDatasetsRandom(conf.getSourcesPath() 
						+ "input_dataset", conf);
			}

			// Count the lines of the datasets
			pt = new Path(conf.getHdfsPath() + "datasets/R");
			fs = pt.getFileSystem();
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			LineNumberReader lnr = new LineNumberReader(br);
			lnr.skip(Long.MAX_VALUE);
			conf.setNr(lnr.getLineNumber() + 1);
			lnr.close();

			pt = new Path(conf.getHdfsPath() + "datasets/S");
			fs = pt.getFileSystem();
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));

			lnr = new LineNumberReader(br);
			lnr.skip(Long.MAX_VALUE);
			conf.setNs(lnr.getLineNumber() + 1);
			lnr.close();

			// Set up the execution environment
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			// ////////////*********** FLINK EXECUTE **************///////////////////
			/******** PHASE 1 ***********/
			DataSet<String> inputR = env.readTextFile(conf.getHdfsPath() + "datasets/R");
			DataSet<String> inputS = env.readTextFile(conf.getHdfsPath() + "datasets/S");

			// ///////// MAP
			DataSet<ZorderRecord> resultR = inputR.flatMap(new MapPhase1(0, conf));
			DataSet<ZorderRecord> resultS = inputS.flatMap(new MapPhase1(1, conf));
			DataSet<ZorderRecord> resultRS = resultR.union(resultS);

			// ///////// REDUCE
			DataSet<String> resultPhase1 = resultRS.groupBy("fourth")
											.reduceGroup(new ReducePhase1(conf));

			/******** PHASE 2 ***********/
			// ///////// MAP
			DataSet<ZorderRecord> result = resultPhase1.flatMap(new MapPhase2(conf));

			// ///////// REDUCE
			DataSet<Tuple4<String, String, String, String>> reduceResultPh2 = result
											.groupBy("fourth").reduceGroup(new ReducePhase2(conf));

			/******** PHASE 3 ***********/
			// ///////// REDUCE
			DataSet<String> reduceResultPh3 = reduceResultPh2.groupBy(0)
											.reduceGroup(new ReducePhase3(conf));

			// ///////// WRITE RESULT
			reduceResultPh3.writeAsText(conf.getHdfsPath() + "FinalKNNresults");

			// execute program
			long startExecuteTime = System.currentTimeMillis();
			env.execute("Flink zkNN");
			long totalElapsedExecuteTime = System.currentTimeMillis() - startExecuteTime;

			// Count execution time
			int ExecuteMillis = (int) totalElapsedExecuteTime % 1000;
			int ExecuteSeconds = (int) (totalElapsedExecuteTime / 1000) % 60;
			int ExecuteMinutes = (int) ((totalElapsedExecuteTime / (1000 * 60)) % 60);
			int ExecuteHours = (int) ((totalElapsedExecuteTime / (1000 * 60 * 60)) % 24);
			System.out.println("Thread " + Thread.currentThread().getId()
					+ " total time: " + ExecuteHours + "h " + ExecuteMinutes
					+ "m " + ExecuteSeconds + "sec " + ExecuteMillis + "mil");

			// Calculate accuracy
			double acc = Functions.calculateAccuracy(conf, i);
			totalPercentage += acc;
		}

		System.out.println("Final accuracy: " + totalPercentage / folds);
	}
}
