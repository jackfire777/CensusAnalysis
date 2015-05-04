package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CensusValueWritable implements Writable {

	private IntWritable numResidencesOwned = new IntWritable(0); // Q1
	private IntWritable numResidencesRented = new IntWritable(0); // Q1
	private IntWritable numMaleNeverMarried = new IntWritable(0); // Q2
	private IntWritable numMaleTotalCalc = new IntWritable(0); // Q3
	private IntWritable numMaleTotal = new IntWritable(0); // Q2
	private IntWritable numFemaleNeverMarried = new IntWritable(0); // Q2
	private IntWritable numFemaleTotalCalc = new IntWritable(0); // Q3
	private IntWritable numFemaleTotal = new IntWritable(0); // Q2
	private IntWritable numMaleBelow18 = new IntWritable(0); // Q3
	private IntWritable numMale19to29 = new IntWritable(0); // Q3
	private IntWritable numMale30to39 = new IntWritable(0); // Q3
	private IntWritable numFemaleBelow18 = new IntWritable(0); // Q3
	private IntWritable numFemale19to29 = new IntWritable(0); // Q3
	private IntWritable numFemale30to39 = new IntWritable(0); // Q3
	private IntWritable numPeepsOver85 = new IntWritable(0); // Q8
	private IntWritable numPeepsTotal = new IntWritable(0); // Q8
	private IntWritable urbanHouseholds = new IntWritable(0); // Q4
	private IntWritable ruralHouseholds = new IntWritable(0); // Q4
	private IntWritable totalHouseholds = new IntWritable(0); // Q4
	private IntWritable[] hhV = new IntWritable[20];
	private IntWritable[] rV = new IntWritable[16];
	private IntWritable[] nR = new IntWritable[9];

	public CensusValueWritable() {
		for (int i = 0; i < hhV.length; i++) {
			hhV[i] = new IntWritable(0);
		}
		for (int i = 0; i < rV.length; i++) {
			rV[i] = new IntWritable(0);
		}
		for (int i = 0; i < nR.length; i++) {
			nR[i] = new IntWritable(0);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		numResidencesOwned.write(out);
		numResidencesRented.write(out);
		numMaleNeverMarried.write(out);
		numMaleTotal.write(out);
		numFemaleNeverMarried.write(out);
		numFemaleTotal.write(out);
		numMaleBelow18.write(out);
		numMale19to29.write(out);
		numMale30to39.write(out);
		numFemaleBelow18.write(out);
		numFemale19to29.write(out);
		numFemale30to39.write(out);
		numPeepsOver85.write(out);
		numPeepsTotal.write(out);
		urbanHouseholds.write(out);
		ruralHouseholds.write(out);
		totalHouseholds.write(out);
		numMaleTotalCalc.write(out);
		numFemaleTotalCalc.write(out);
		for (int i = 0; i < hhV.length; i++) {
			hhV[i].write(out);
		}
		for (int i = 0; i < rV.length; i++) {
			rV[i].write(out);
		}
		for (int i = 0; i < nR.length; i++) {
			nR[i].write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		numResidencesOwned.readFields(in);
		numResidencesRented.readFields(in);
		numMaleNeverMarried.readFields(in);
		numMaleTotal.readFields(in);
		numFemaleNeverMarried.readFields(in);
		numFemaleTotal.readFields(in);
		numMaleBelow18.readFields(in);
		numMale19to29.readFields(in);
		numMale30to39.readFields(in);
		numFemaleBelow18.readFields(in);
		numFemale19to29.readFields(in);
		numFemale30to39.readFields(in);
		numPeepsOver85.readFields(in);
		numPeepsTotal.readFields(in);
		urbanHouseholds.readFields(in);
		ruralHouseholds.readFields(in);
		totalHouseholds.readFields(in);
		numMaleTotalCalc.readFields(in);
		numFemaleTotalCalc.readFields(in);
		for (int i = 0; i < hhV.length; i++) {
			hhV[i].readFields(in);
		}
		for (int i = 0; i < rV.length; i++) {
			rV[i].readFields(in);
		}
		for (int i = 0; i < nR.length; i++) {
			nR[i].readFields(in);
		}
	}

	public void combineValues(CensusValueWritable rhs) {
		numResidencesOwned.set(numResidencesOwned.get()
				+ rhs.getNumResidencesOwned().get());
		numResidencesRented.set(numResidencesRented.get()
				+ rhs.getNumResidencesRented().get());
		numMaleTotal.set(numMaleTotal.get() + rhs.getNumMaleTotal().get());
		numMaleNeverMarried.set(numMaleNeverMarried.get()
				+ rhs.getNumMaleNeverMarried().get());
		numFemaleTotal
				.set(numFemaleTotal.get() + rhs.getNumFemaleTotal().get());
		numFemaleNeverMarried.set(numFemaleNeverMarried.get()
				+ rhs.getNumFemaleNeverMarried().get());
		numMaleBelow18
				.set(numMaleBelow18.get() + rhs.getNumMaleBelow18().get());
		numMale19to29.set(numMale19to29.get() + rhs.getNumMale19to29().get());
		numMale30to39.set(numMale30to39.get() + rhs.getNumMale30to39().get());
		numFemaleBelow18.set(numFemaleBelow18.get()
				+ rhs.getNumFemaleBelow18().get());
		numFemale19to29.set(numFemale19to29.get()
				+ rhs.getNumFemale19to29().get());
		numFemale30to39.set(numFemale30to39.get()
				+ rhs.getNumFemale30to39().get());
		numPeepsOver85
				.set(numPeepsOver85.get() + rhs.getNumPeepsOver85().get());
		numPeepsTotal.set(numPeepsTotal.get() + rhs.getNumPeepsTotal().get());
		urbanHouseholds.set(urbanHouseholds.get()
				+ rhs.getUrbanHouseholds().get());
		ruralHouseholds.set(ruralHouseholds.get()
				+ rhs.getRuralHouseholds().get());
		totalHouseholds.set(totalHouseholds.get()
				+ rhs.getTotalHouseholds().get());
		numMaleTotalCalc.set(numMaleTotalCalc.get()
				+ rhs.numMaleTotalCalc.get());
		numFemaleTotalCalc.set(numFemaleTotalCalc.get()
				+ rhs.numFemaleTotalCalc.get());
		for (int i = 0; i < hhV.length; i++) {
			hhV[i].set(hhV[i].get() + rhs.getHouseholdValues()[i].get());
		}
		for (int i = 0; i < rV.length; i++) {
			rV[i].set(rV[i].get() + rhs.getRentValues()[i].get());
		}
		for (int i = 0; i < nR.length; i++) {
			nR[i].set(nR[i].get() + rhs.getNumRooms()[i].get());
		}
	}

	public void clear() {
		numResidencesOwned.set(0);
		numResidencesRented.set(0);
		numMaleNeverMarried.set(0);
		numMaleTotal.set(0);
		numFemaleNeverMarried.set(0);
		numFemaleTotal.set(0);
		numMaleBelow18.set(0);
		numMale19to29.set(0);
		numMale30to39.set(0);
		numFemaleBelow18.set(0);
		numFemale19to29.set(0);
		numFemale30to39.set(0);
		numPeepsOver85.set(0);
		numPeepsTotal.set(0);
		urbanHouseholds.set(0);
		ruralHouseholds.set(0);
		totalHouseholds.set(0);
		numMaleTotalCalc.set(0);
		numFemaleTotalCalc.set(0);
		for (int i = 0; i < 20; i++) {
			hhV[i].set(0);
		}
		for (int i = 0; i < 16; i++) {
			rV[i].set(0);
		}
		for (int i = 0; i < 9; i++) {
			nR[i].set(0);
		}
	}

	@Override
	public String toString() {
		return "Didn't implement toString";
	}

	public IntWritable getNumResidencesOwned() {
		return numResidencesOwned;
	}

	public void setNumResidencesOwned(IntWritable numResidencesOwned) {
		this.numResidencesOwned.set(numResidencesOwned.get());
	}

	public IntWritable getNumResidencesRented() {
		return numResidencesRented;
	}

	public void setNumResidencesRented(IntWritable numResidencesRented) {
		this.numResidencesRented.set(numResidencesRented.get());
	}

	public IntWritable getNumMaleNeverMarried() {
		return numMaleNeverMarried;
	}

	public void setNumMaleNeverMarried(IntWritable numMaleMarried) {
		this.numMaleNeverMarried.set(numMaleMarried.get());
	}

	public IntWritable getNumMaleTotal() {
		return numMaleTotal;
	}

	public void setNumMaleTotal(IntWritable numMaleTotal) {
		this.numMaleTotal.set(numMaleTotal.get());
	}

	public IntWritable getNumFemaleNeverMarried() {
		return numFemaleNeverMarried;
	}

	public void setNumFemaleNeverMarried(IntWritable numFemaleNeverMarried) {
		this.numFemaleNeverMarried.set(numFemaleNeverMarried.get());
	}

	public IntWritable getNumFemaleTotal() {
		return numFemaleTotal;
	}

	public void setNumFemaleTotal(IntWritable numFemaleTotal) {
		this.numFemaleTotal.set(numFemaleTotal.get());
	}

	public IntWritable getNumMaleBelow18() {
		return numMaleBelow18;
	}

	public void setNumMaleBelow18(IntWritable numMaleBelow18) {
		this.numMaleBelow18.set(numMaleBelow18.get());
	}

	public IntWritable getNumMale19to29() {
		return numMale19to29;
	}

	public void setNumMale19to29(IntWritable numMale19to29) {
		this.numMale19to29.set(numMale19to29.get());
	}

	public IntWritable getNumMale30to39() {
		return numMale30to39;
	}

	public void setNumMale30to39(IntWritable numMale30to39) {
		this.numMale30to39.set(numMale30to39.get());
	}

	public IntWritable getNumFemaleBelow18() {
		return numFemaleBelow18;
	}

	public void setNumFemaleBelow18(IntWritable numFemaleBelow18) {
		this.numFemaleBelow18.set(numFemaleBelow18.get());
	}

	public IntWritable getNumFemale19to29() {
		return numFemale19to29;
	}

	public void setNumFemale19to29(IntWritable numFemale19to29) {
		this.numFemale19to29.set(numFemale19to29.get());
	}

	public IntWritable getNumFemale30to39() {
		return numFemale30to39;
	}

	public void setNumFemale30to39(IntWritable numFemale30to39) {
		this.numFemale30to39.set(numFemale30to39.get());
	}

	public IntWritable getNumPeepsTotal() {
		return numPeepsTotal;
	}

	public void setNumPeepsTotal(IntWritable numPeepsTotal) {
		this.numPeepsTotal.set(numPeepsTotal.get());
	}

	public IntWritable getUrbanHouseholds() {
		return urbanHouseholds;
	}

	public void setUrbanHouseholds(IntWritable urbanHouseHolds) {
		this.urbanHouseholds.set(urbanHouseHolds.get());
	}

	public IntWritable getRuralHouseholds() {
		return ruralHouseholds;
	}

	public void setRuralHouseholds(IntWritable ruralHouseholds) {
		this.ruralHouseholds.set(ruralHouseholds.get());
	}

	public IntWritable getTotalHouseholds() {
		return totalHouseholds;
	}

	public void setTotalHouseholds(IntWritable totalHouseholds) {
		this.totalHouseholds.set(totalHouseholds.get());
	}

	public IntWritable[] getHouseholdValues() {
		return hhV;
	}

	public void setHouseholdValues(IntWritable householdValue, int index) {
		this.hhV[index].set(householdValue.get());
	}

	public IntWritable[] getRentValues() {
		return rV;
	}

	public void setRentValues(IntWritable rentValue, int index) {
		this.rV[index].set(rentValue.get());
	}

	public IntWritable[] getNumRooms() {
		return nR;
	}

	public void setNumRooms(IntWritable numRooms, int index) {
		this.nR[index].set(numRooms.get());
	}

	public IntWritable getNumPeepsOver85() {
		return numPeepsOver85;
	}

	public void setNumPeepsOver85(IntWritable numPeepsOver85Total) {
		this.numPeepsOver85.set(numPeepsOver85Total.get());
	}

	public IntWritable getNumMaleTotalCalc() {
		return numMaleTotalCalc;
	}

	public void setNumMaleTotalCalc(IntWritable numMaleTotalCalc) {
		this.numMaleTotalCalc.set(numMaleTotalCalc.get());
	}

	public IntWritable getNumFemaleTotalCalc() {
		return numFemaleTotalCalc;
	}

	public void setNumFemaleTotalCalc(IntWritable numFemaleTotalCalc) {
		this.numFemaleTotalCalc.set(numFemaleTotalCalc.get());
	}

}
