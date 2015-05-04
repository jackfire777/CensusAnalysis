//Author: Jordan Messec
//Date: 4/22/15
//Email: jmess4@gmail.com
package writables;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class StateByState {

	private Text state;
	private FloatWritable percentRented = new FloatWritable(); // Q1
	private FloatWritable percentOwned = new FloatWritable(); // Q1
	private FloatWritable percentMaleNeverMarried = new FloatWritable(); // Q2
	private FloatWritable percentFemaleNeverMarried = new FloatWritable(); // Q2
	private FloatWritable percentMaleUnder18 = new FloatWritable(); // Q3a
	private FloatWritable percentFemaleUnder18 = new FloatWritable(); // Q3a
	private FloatWritable percentMale19to29 = new FloatWritable(); // Q3a
	private FloatWritable percentFemale19to29 = new FloatWritable(); // Q3a
	private FloatWritable percentMale30to39 = new FloatWritable(); // Q3a
	private FloatWritable percentFemale30to39 = new FloatWritable(); // Q3a
	private FloatWritable percentRural = new FloatWritable(); // Q4
	private FloatWritable percentUrban = new FloatWritable(); // Q4
	private Text[] medianValueRanges = { new Text("Less than $15,000"),
			new Text("$15,000 - $19,999"), new Text("$20,000 - $24,999"),
			new Text("$25,000-$29,999"), new Text("$30,000 - $34,999"),
			new Text("$35,000 - $39,999"), new Text("$40,000 - $44,999"),
			new Text("$45,000 - $49,999"), new Text("$50,000 - $59,999"),
			new Text("$60,000 - $74,999"), new Text("$75,000 - $99,999"),
			new Text("$100,000 - $124,999"), new Text("$125,000 - $149,999"),
			new Text("$150,000 - $174,999"), new Text("$175,000 - $199,999"),
			new Text("$200,000 - $249,999"), new Text("$250,000 - $299,999"),
			new Text("$300,000 - $399,999"), new Text("$400,000 - $499,999"),
			new Text("$500,000 or more") }; // Q5
	private IntWritable medianValue = new IntWritable(0); // Q5
	private Text[] medianRentRanges = { new Text("Less than $100"),
			new Text("$100 - $149"), new Text("$150 - $199"),
			new Text("$200 - $249"), new Text("$250 - $299"),
			new Text("$300 - $349"), new Text("$350 - $399"),
			new Text("$400 - $449"), new Text("$450 - $499"),
			new Text("$500 - $549"), new Text("$550 - $599"),
			new Text("$600 - $649"), new Text("$650 - $699"),
			new Text("$700 - $749"), new Text("$750 - $999"),
			new Text("$1000 or more") }; // Q5
	private IntWritable medianRent = new IntWritable(0); // Q6

	public StateByState() {

	}

	public void setValues(Text state, IntWritable rented, IntWritable owned,
			IntWritable maleNeverMarried, IntWritable totalMale,
			IntWritable femaleNeverMarried, IntWritable totalFemale,
			IntWritable maleUnder18, IntWritable femaleUnder18,
			IntWritable male19to29, IntWritable female19to29,
			IntWritable male30to39, IntWritable female30to39,
			IntWritable urbanHH, IntWritable ruralHH, IntWritable totalHH,
			IntWritable[] medianValues, IntWritable[] medianRents,
			IntWritable totalMaleCalc, IntWritable totalFemaleCalc) {
		this.state = state;
		this.percentRented.set(((float) rented.get())
				/ ((float) rented.get() + (float) owned.get()) * 100);
		this.percentOwned.set((float) 100.0 - this.percentRented.get());
		this.percentMaleNeverMarried.set((float) maleNeverMarried.get()
				/ (float) totalMale.get() * 100);
		this.percentFemaleNeverMarried.set((float) femaleNeverMarried.get()
				/ (float) totalFemale.get() * 100);
		this.percentMaleUnder18.set((float) maleUnder18.get()
				/ (float) totalMaleCalc.get() * 100);
		this.percentFemaleUnder18.set((float) femaleUnder18.get()
				/ (float) totalFemaleCalc.get() * 100);
		this.percentMale19to29.set((float) male19to29.get()
				/ (float) totalMaleCalc.get() * 100);
		this.percentFemale19to29.set((float) female19to29.get()
				/ (float) totalFemaleCalc.get() * 100);
		this.percentMale30to39.set((float) male30to39.get()
				/ (float) totalMaleCalc.get() * 100);
		this.percentFemale30to39.set((float) female30to39.get()
				/ (float) totalFemaleCalc.get() * 100);
		this.percentUrban.set((float) urbanHH.get() / (float) totalHH.get()
				* 100);
		this.percentRural.set((float) ruralHH.get() / (float) totalHH.get()
				* 100);

		for (int i = 0; i < medianValues.length; i++) {
			medianValue.set(medianValue.get() + medianValues[i].get());
		}
		medianValue.set(medianValue.get() / 2);
		int sum = 0;
		for (int i = 0; i < medianValues.length; i++) {
			sum += medianValues[i].get();
			if (sum > medianValue.get()) {
				medianValue.set(i);
				break;
			}
		}

		for (int i = 0; i < medianRents.length; i++) {
			medianRent.set(medianRent.get() + medianRents[i].get());
		}
		medianRent.set(medianRent.get() / 2);
		sum = 0;
		for (int i = 0; i < medianRents.length; i++) {
			sum += medianRents[i].get();
			if (sum > medianRent.get()) {
				medianRent.set(i);
				break;
			}
		}
	}

	@Override
	public String toString() {
		return state.toString() + "\n" + "  Q1:\t" + percentRented.get()
				+ "% of residences were rented." + "\n\t" + percentOwned.get()
				+ "% of residences were owned." + "\n  Q2:\t"
				+ percentMaleNeverMarried.get() + "% of men never married."
				+ "\n\t" + percentFemaleNeverMarried.get()
				+ "% of women never married." + "\n  Q3:\t"
				+ percentMaleUnder18.get()
				+ "% of men were 18 years old or younger." + "\n\t"
				+ percentFemaleUnder18.get()
				+ "% of women were 18 years old or younger." + "\n\t"
				+ percentMale19to29.get() + "% of men were age 19-29." + "\n\t"
				+ percentFemale19to29.get() + "% of women were age 19-29."
				+ "\n\t" + percentMale30to39.get() + "% of men were age 30-39."
				+ "\n\t" + percentFemale30to39.get()
				+ "% of women were age 30-39." + "\n  Q4:\t"
				+ percentRural.get() + "% of households were rural." + "\n\t"
				+ percentUrban.get() + "% of households were urban."
				+ "\n  Q5:\t" + medianValueRanges[medianValue.get()].toString()
				+ " was the median value of a house occupied by owners."
				+ "\n  Q6:\t" + medianRentRanges[medianRent.get()].toString()
				+ " was the median rent paid by households.";
		// TODO finish
	}

}
