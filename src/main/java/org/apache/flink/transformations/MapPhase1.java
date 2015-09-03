package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.ZorderRecord;
import org.apache.flink.tools.Zorder;
import org.apache.flink.util.Collector;

public class MapPhase1 extends RichFlatMapFunction<String, ZorderRecord>
		implements FlatMapFunction<String, ZorderRecord> {

	private static final long serialVersionUID = 1L;
	private int fileId = 0;
	private ExecConf conf;

	public MapPhase1(int fileId, ExecConf conf) {
		this.fileId = fileId;
		this.conf = conf;
	}

	@Override
	public void flatMap(String input, Collector<ZorderRecord> output)
			throws Exception {

		String zval = null;
		String line = input;
		char ch = ',';
		int pos = line.indexOf(ch);
		String id = line.substring(0, pos);
		String rest = line.substring(pos + 1, line.length()).trim();
		String[] parts = rest.split(",");
		float[] coord = new float[conf.getDimension()];
		for (int i = 0; i < conf.getDimension(); i++) {
			float tmp = 0;
			try {
				tmp = Float.valueOf(parts[i]);
			} catch (NumberFormatException e) {
				continue;
			}
			if (tmp > -9999)
				coord[i] = Float.valueOf(parts[i]);
		}

		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		for (int i = 0; i < conf.getShift(); i++) {
			float[] tmp_coord = new float[conf.getDimension()];
			int[] converted_coord = new int[conf.getDimension()];
			for (int k = 0; k < conf.getDimension(); k++) {
				tmp_coord[k] = coord[k];
				converted_coord[k] = (int) tmp_coord[k]; 
				tmp_coord[k] -= converted_coord[k];
				converted_coord[k] *= conf.getScale()[k];
				converted_coord[k] += (tmp_coord[k] * conf.getScale()[k]);
				if (i != 0) converted_coord[k] += conf.getShiftvectors()[i][k];
			}

			zval = Zorder.valueOf(conf.getDimension(), converted_coord);
			
			output.collect(new ZorderRecord(zval, id, fileId, i, Math
					.round(Float.valueOf(parts[conf.getDimension()]))));
		}
	}
}