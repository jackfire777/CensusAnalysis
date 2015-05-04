package job;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import writables.CensusAnswers78;
import writables.CensusAnswers;
import writables.CensusValueWritable;

public class CensusReducer extends
		Reducer<Text, CensusValueWritable, CensusAnswers78, NullWritable> {

	private NullWritable nW = NullWritable.get();
	private CensusValueWritable cvwSum = new CensusValueWritable();
	private CensusAnswers censusAnswers = new CensusAnswers();
	private CensusAnswers78 cA78W = new CensusAnswers78();
	@SuppressWarnings("rawtypes")
	private MultipleOutputs mOuts;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void setup(Context context) {
		mOuts = new MultipleOutputs(context);
	}

	/*
	 * Input: <Text, [CVW1, CVW2,...]> e.g. <CO, [CVW1, CVW2,...]>
	 * 
	 * Output: mOuts writes a file named "Q1toQ6part-r-00000" which contains
	 * Q1-6 answers. Context writes a file named part-r-00000 which contains
	 * Q7,8 answers.
	 */
	@SuppressWarnings("unchecked")
	protected void reduce(Text key, Iterable<CensusValueWritable> values,
			Context context) throws IOException, InterruptedException {
		cvwSum.clear();
		for (CensusValueWritable rhs : values) {
			cvwSum.combineValues(rhs);
		}

		cA78W.setValues(key, cvwSum.getNumPeepsOver85(),
				cvwSum.getNumPeepsTotal(), cvwSum.getNumRooms());

		censusAnswers.setValues(key, cvwSum.getNumResidencesRented(),
				cvwSum.getNumResidencesOwned(),
				cvwSum.getNumMaleNeverMarried(), cvwSum.getNumMaleTotal(),
				cvwSum.getNumFemaleNeverMarried(), cvwSum.getNumFemaleTotal(),
				cvwSum.getNumMaleBelow18(), cvwSum.getNumFemaleBelow18(),
				cvwSum.getNumMale19to29(), cvwSum.getNumFemale19to29(),
				cvwSum.getNumMale30to39(), cvwSum.getNumFemale30to39(),
				cvwSum.getUrbanHouseholds(), cvwSum.getRuralHouseholds(),
				cvwSum.getTotalHouseholds(), cvwSum.getHouseholdValues(),
				cvwSum.getRentValues(), cvwSum.getNumMaleTotalCalc(),
				cvwSum.getNumFemaleTotalCalc());

		try {
			mOuts.write("Q1toQ6", censusAnswers, nW);
		} catch (IOException e1) {
			System.err.println(e1.getMessage() + "JMESS mapper mOuts JMESS");
		} catch (InterruptedException e1) {
			System.err.println(e1.getMessage() + "JMESS mapper mOuts JMESS");
		}
	}

	@Override
	public void cleanup(Context context) {
		try {
			cA78W.findPercentile();
			context.write(cA78W, nW);
			mOuts.close();
		} catch (IOException e) {
			System.err.println(e.getMessage()
					+ "JMESS trouble closing MultipleOutputs object");
		} catch (InterruptedException e) {
			System.err.println(e.getMessage()
					+ "JMESS trouble closing MultipleOutputs object");
		}
	}
}