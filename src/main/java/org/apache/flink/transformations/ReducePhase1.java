package org.apache.flink.transformations;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.io.File;
import java.util.Comparator;
import java.io.OutputStreamWriter;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.Zorder;
import org.apache.flink.tools.ZorderRecord;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.tools.Functions;
import org.apache.flink.util.Collector;

public class ReducePhase1 extends RichGroupReduceFunction<ZorderRecord, String> 
						implements GroupReduceFunction<ZorderRecord, String> {

	private static final long serialVersionUID = 1L;
	private Random r;
	private double sampleRate;
	private static double sampleRateOfR;
	private static double sampleRateOfS;
	private ExecConf conf;

	public ReducePhase1(ExecConf conf) {
		this.conf = conf;
	}

	@Override
	public void reduce(Iterable<ZorderRecord> input, Collector<String> output)
			throws Exception {

		ArrayList<String> RtmpList = new ArrayList<String>();
		ArrayList<String> StmpList = new ArrayList<String>();

		// *************************** Sampling ****************************//
		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		sampleRateOfR = 1 / (conf.getEpsilon() * conf.getEpsilon() * conf.getNr());
		sampleRateOfS = 1 / (conf.getEpsilon() * conf.getEpsilon() * conf.getNs());

		r = new Random();
		if (sampleRateOfR > 1)
			sampleRateOfR = 1;
		if (sampleRateOfS > 1)
			sampleRateOfS = 1;

		if (sampleRateOfR * conf.getNr() < 1) {
			System.out.printf("Increase sampling rate of R :  " + sampleRateOfR);
			System.exit(-1);
		}

		if (sampleRateOfS * conf.getNs() < 1) {
			System.out.printf("Increase sampling rate of R :  " + sampleRateOfS);
			System.exit(-1);
		}
		String shiftId = "";

		FileWriter intermediateWriter = new FileWriter(conf.getLocalPath() 
						+ "Intermediate_ReducePhase1"
						+ Thread.currentThread().getId(), true);
		Iterator<ZorderRecord> iterator = input.iterator();
		while (iterator.hasNext()) {
			ZorderRecord entry = iterator.next();
			intermediateWriter.write(entry.toString() + "\n");
			if (entry.getThird() == 0)
				sampleRate = sampleRateOfR;
			else if (entry.getThird() == 1)
				sampleRate = sampleRateOfS;
			else {
				System.out.println("Wrong source file!");
				System.exit(-1);
			}
			boolean sampled = false;
			if (r.nextDouble() < sampleRate)
				sampled = true;
			if (sampled) {
				if (entry.getThird() == 0) {
					RtmpList.add(entry.getFirst());
					shiftId = entry.getFourth().toString();
				} else {
					StmpList.add(entry.getFirst());
					shiftId = entry.getFourth().toString();
				}
			} else {
				continue;
			}
		}
		intermediateWriter.close();

		int Rsize = RtmpList.size();
		int Ssize = StmpList.size();

		ValueComparator com = new ValueComparator();
		Collections.sort(RtmpList, com);
		Collections.sort(StmpList, com);

		String q_start = "";
		int len = Zorder.maxDecDigits(conf.getDimension());
		q_start = Functions.createExtra(len);

		Path rPt = new Path(conf.getHdfsPath() + "Rrange" + shiftId);
		Path sPt = new Path(conf.getHdfsPath() + "Srange" + shiftId);

		FileSystem fs = rPt.getFileSystem();
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(rPt, true)));
		fs = sPt.getFileSystem();
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(sPt, true)));

		// *************************** Estimate ranges ****************************//
		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		for (int i = 1; i <= conf.getNumOfPartition(); i++) {

			int estRank = Functions.getEstimatorIndex(i, conf.getNr(),
					sampleRateOfR, conf.getNumOfPartition());
			if (estRank - 1 >= Rsize)
				estRank = Rsize;

			String q_end;
			if (i == conf.getNumOfPartition()) {
				q_end = Zorder.maxDecString(conf.getDimension());
			} else
				q_end = RtmpList.get(estRank - 1);

			rBw.write("(" + shiftId + "," + q_start + " " + q_end + ")\n");

			int low;
			if (i == 1)
				low = 0;
			else {
				int newKnn = (int) Math
						.ceil((double) conf.getKnn()
								/ (conf.getEpsilon() * conf.getEpsilon() * conf
										.getNs()));
				low = Collections.binarySearch(StmpList, q_start);
				if (low < 0)
					low = -low - 1;
				if ((low - newKnn) < 0)
					low = 0;
				else
					low -= newKnn;
			}

			String s_start;
			if (i == 1) {
				len = Zorder.maxDecDigits(conf.getDimension());
				s_start = Functions.createExtra(len);
			} else
				s_start = StmpList.get(low);

			int high;
			if (i == conf.getNumOfPartition()) {
				high = Ssize - 1;
			} else {
				int newKnn = (int) Math
						.ceil((double) conf.getKnn()
								/ (conf.getEpsilon() * conf.getEpsilon() * conf
										.getNs()));
				high = Collections.binarySearch(StmpList, q_end);
				if (high < 0)
					high = -high - 1;
				if ((high + newKnn) > Ssize - 1)
					high = Ssize - 1;
				else
					high += newKnn;
			}

			String s_end;
			if (i == conf.getNumOfPartition()) {
				s_end = Zorder.maxDecString(conf.getDimension());
			} else {
				s_end = StmpList.get(high);
			}

			sBw.write("(" + shiftId + "," + s_start + " " + s_end + ")\n");

			q_start = q_end;
		}
		sBw.close();
		rBw.close();

		BufferedReader br = new BufferedReader(
				new FileReader(conf.getLocalPath() + "Intermediate_ReducePhase1"
								+ Thread.currentThread().getId()), 1024);
		while (true) {
			String l = br.readLine();
			if (l == null)
				break;
			output.collect(l);
		}
		br.close();
		File tmp = new File(conf.getLocalPath() + "Intermediate_ReducePhase1"
						+ Thread.currentThread().getId());
		tmp.delete();
	}

	private class ValueComparator implements Comparator<String> {

		@Override
		public int compare(String w1, String w2) {

			int cmp = w1.compareTo(w2);
			if (cmp != 0)
				return cmp;
			cmp = w1.toString().compareTo(w2.toString());

			return cmp;
		}
	}
}
