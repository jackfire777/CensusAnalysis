//Author: Jordan Messec
//Date: 4/22/15
//Email: jmess4@gmail.com
package inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import writables.CensusValueWritable;

class CensusDataLineRecordReader extends
		RecordReader<Text, CensusValueWritable> {

	private CensusValueWritable value;
	private Text key;
	private IntWritable valToSend = new IntWritable();
	private IntWritable valCounter = new IntWritable();
	private LineRecordReader reader;
	Path splitPath;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException {
		splitPath = ((FileSplit) inputSplit).getPath();
		reader = new LineRecordReader();
		reader.initialize(inputSplit, context);
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public CensusValueWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		String line;

		while (true) {
			if (reader.nextKeyValue()) {
				line = reader.getCurrentValue().toString();
			} else {
				return false;
			}
			if (line == null) {
				return false;
			}

			if (key == null) {
				key = new Text();
			}
			if (value == null) {
				value = new CensusValueWritable();
			}

			int summaryLevel = Integer.parseInt(line.substring(10, 13));
			if (summaryLevel != 100) { // we only want records of summary level
										// 100, discard if not
				continue;
			}

			if (line.charAt(27) == '1') {
				key.set(line.substring(8, 10));

				// Male: Never Married, Q2
				valToSend.set(Integer.parseInt(line.substring(4422, 4431)));
				value.setNumMaleNeverMarried(valToSend);

				// Male: Total, Q2
				valToSend.set(Integer.parseInt(line.substring(363, 372)));
				value.setNumMaleTotal(valToSend);

				// Female: Never Married, Q2
				valToSend.set(Integer.parseInt(line.substring(4467, 4476)));
				value.setNumFemaleNeverMarried(valToSend);

				// Female: Total, Q2
				valToSend.set(Integer.parseInt(line.substring(372, 381)));
				value.setNumFemaleTotal(valToSend);

				// calculate males in age brackets, Q3
				valToSend.set(0);
				valCounter.set(0);
				int temp = 0;
				for (int i = 0; i < 31; i++) {
					temp = Integer.parseInt(line.substring(3864 + i * 9,
							3873 + i * 9));
					valCounter.set(valCounter.get() + temp);
					valToSend.set(valToSend.get() + temp);
					if (i == 12) { // <18
						value.setNumMaleBelow18(valToSend);
						valToSend.set(0);
					}
					if (i == 17) { // 19-29
						value.setNumMale19to29(valToSend);
						valToSend.set(0);
					}
					if (i == 19) { // 30-39
						value.setNumMale30to39(valToSend);
						valToSend.set(0);
					}
				}
				value.setNumMaleTotalCalc(valCounter); // total males

				// calculate females in age brackets, Q3
				valToSend.set(0);
				valCounter.set(0);
				temp = 0;
				for (int i = 0; i < 31; i++) {
					temp = Integer.parseInt(line.substring(4143 + i * 9,
							4152 + i * 9));
					valCounter.set(valCounter.get() + temp);
					valToSend.set(valToSend.get() + temp);
					if (i == 12) { // <18
						value.setNumFemaleBelow18(valToSend);
						valToSend.set(0);
					}
					if (i == 17) { // 19-29
						value.setNumFemale19to29(valToSend);
						valToSend.set(0);
					}
					if (i == 19) { // 30-39
						value.setNumFemale30to39(valToSend);
						valToSend.set(0);
					}
				}
				value.setNumFemaleTotalCalc(valCounter); // total females

				// listed totals for Men, Q2
				valToSend.set(Integer.parseInt(line.substring(363, 372)));
				value.setNumMaleTotal(valToSend);

				// listed totals for Women, Q2
				valToSend.set(Integer.parseInt(line.substring(372, 381)));
				value.setNumFemaleTotal(valToSend);

				// People >=85, Q8
				valToSend.set(Integer.parseInt(line.substring(1065, 1074)));
				value.setNumPeepsOver85(valToSend);

				// Total people, Q8
				valToSend.set(Integer.parseInt(line.substring(300, 309)));
				value.setNumPeepsTotal(valToSend);

				continue;
			} else { // second segment
				// Owner Occupied, Q1
				valToSend.set(Integer.parseInt(line.substring(1803, 1812)));
				value.setNumResidencesOwned(valToSend);

				// Renter Occupied, Q1
				valToSend.set(Integer.parseInt(line.substring(1812, 1821)));
				value.setNumResidencesRented(valToSend);

				// Urban Households, Q4
				valToSend.set(Integer.parseInt(line.substring(1857, 1866))
						+ Integer.parseInt(line.substring(1866, 1875)));
				value.setUrbanHouseholds(valToSend);

				int totalHouseholds = valToSend.get(); // sum total num
														// households

				// Rural Households, Q4
				valToSend.set(Integer.parseInt(line.substring(1875, 1884)));
				value.setRuralHouseholds(valToSend);
				totalHouseholds += valToSend.get();

				// Unclassified Households, but actually send TOTAL households,
				// Q4
				valToSend.set(Integer.parseInt(line.substring(1884, 1893)));
				valToSend.set(valToSend.get() + totalHouseholds);
				value.setTotalHouseholds(valToSend);

				// Household values, Q5
				for (int i = 0; i < 20; i++) {
					valToSend.set(Integer.parseInt(line.substring(2928 + i * 9,
							2937 + i * 9)));
					value.setHouseholdValues(valToSend, i);
				}

				// Rent paid, Q6
				for (int i = 0; i < 16; i++) {
					valToSend.set(Integer.parseInt(line.substring(3450 + i * 9,
							3459 + i * 9)));
					value.setRentValues(valToSend, i);
				}

				// Rooms per house, Q7
				for (int i = 0; i < 9; i++) {
					valToSend.set(Integer.parseInt(line.substring(2388 + i * 9,
							2397 + i * 9)));
					value.setNumRooms(valToSend, i);
				}

				// completed filling a CensusValueWritable
				return true;
			}
		}

	}

}