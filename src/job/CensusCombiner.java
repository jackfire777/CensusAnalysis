//Author: Jordan Messec
//Date: 4/22/15
//Email: jmess4@gmail.com
package job;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writables.CensusValueWritable;

public class CensusCombiner extends
		Reducer<Text, CensusValueWritable, Text, CensusValueWritable> {

	private CensusValueWritable cvwSum = new CensusValueWritable();

	/*
	 * Input: <Text, [CVW1, CVW2,...]> e.g. <CO, [CVW1, CVW2,...]>
	 * 
	 * Output: <Text, CWVsums> e.g. <CO, CVWtotals>
	 */
	protected void reduce(Text key, Iterable<CensusValueWritable> values,
			Context context) throws IOException, InterruptedException {
		cvwSum.clear();
		for (CensusValueWritable rhs : values) {
			cvwSum.combineValues(rhs);
		}
		context.write(key, cvwSum);
	}
}
