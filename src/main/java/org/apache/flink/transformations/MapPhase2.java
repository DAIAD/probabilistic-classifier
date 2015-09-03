package org.apache.flink.transformations;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.InputStreamReader;

import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.ZorderRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class MapPhase2 extends RichFlatMapFunction<String, ZorderRecord>
		implements FlatMapFunction<String, ZorderRecord> {

	private static final long serialVersionUID = 1L;
	private String[] RrangeArray;
	private String[] SrangeArray;
	private ExecConf conf;

	public MapPhase2(ExecConf conf) throws IOException {
		this.conf = conf;
	}

	@Override
	public void flatMap(String input, Collector<ZorderRecord> output)
			throws Exception {

		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		String line = input.trim();
		String[] parts = line.split(" ");
		int zOffset, ridOffset, srcOffset, sidOffset, classOffset;
		zOffset = 0;
		ridOffset = zOffset + 1;
		srcOffset = ridOffset + 1;
		sidOffset = srcOffset + 1;
		classOffset = sidOffset + 1;

		ArrayList<String> pidList = getPartitionId(parts[zOffset],
				parts[srcOffset], parts[sidOffset]);
		if (pidList.size() == 0) {
			System.out.println("Cannot get pid");
			System.exit(-1);
		}

		for (int i = 0; i < pidList.size(); i++) {
			String pid = pidList.get(i);
			int intSid = Integer.valueOf(parts[sidOffset]);
			int intPid = Integer.valueOf(pid);
			int groupKey = intSid * conf.getNumOfPartition() + intPid;

			ZorderRecord bp2v = new ZorderRecord(parts[zOffset],
					parts[ridOffset], Integer.parseInt(parts[srcOffset]),
					groupKey, Math.round(Float.parseFloat(parts[classOffset])));
			output.collect(bp2v);
		}
	}

	/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
	public ArrayList<String> getPartitionId(String z, String src, String sid)
			throws IOException {

		if ((RrangeArray == null) && (SrangeArray == null)) {
			RrangeArray = new String[conf.getNumOfPartition()];
			SrangeArray = new String[conf.getNumOfPartition()];
			Path pt = new Path(conf.getHdfsPath() + "Rrange" + sid);
			
			FileSystem fs = pt.getFileSystem();
			BufferedReader rBr = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			pt = new Path(conf.getHdfsPath() + "Srange" + sid);
			fs = pt.getFileSystem();
			BufferedReader sBr = new BufferedReader(new InputStreamReader(
					fs.open(pt)));

			int counter = 0;
			while (true) {
				String lineR = rBr.readLine();
				if (lineR == null)
					break;
				RrangeArray[counter] = lineR.trim();

				String lineS = sBr.readLine();
				if (lineS == null)
					break;
				SrangeArray[counter] = lineS.trim();
				counter++;
			}

			rBr.close();
			sBr.close();
		}

		String ret = null;
		String[] mark = null;
		ArrayList<String> idList = new ArrayList<String>(
				conf.getNumOfPartition());

		if (src.compareTo("0") == 0)
			mark = RrangeArray;
		else if (src.compareTo("1") == 0)
			mark = SrangeArray;
		else {
			System.out.println(src);
			System.out.println("Unknown source for input record !!!");
			System.exit(-1);
		}

		for (int i = 0; i < conf.getNumOfPartition(); i++) {
			String range = mark[i];
			String[] parts = null;
			parts = range.split(" +");
			String low = parts[0].substring(3);
			String high = parts[1].substring(0, parts[1].length() - 1);

			if (z.compareTo(low) >= 0 && z.compareTo(high) <= 0) {
				ret = Integer.toString(i);
				idList.add(ret);
			}
		}
		return idList;
	}
}
