//Author: Jordan Messec
//Date: 4/22/15
//Email: jmess4@gmail.com
package writables;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class CensusNationwide {

	private Text stateWithMostOld = new Text();
	private FloatWritable mostOld = new FloatWritable(0);
	private ArrayList<Float> avgRooms = new ArrayList<>();
	private int index = 0;

	public CensusNationwide() {

	}

	public void setValues(Text state, IntWritable numOld, IntWritable numTotal,
			IntWritable[] numRooms) {
		float val = 0, val2 = 0;
		val = (float) numOld.get() / (float) numTotal.get();
		if (val > mostOld.get()) {
			stateWithMostOld.set(state.toString());
			mostOld.set(val);
		}

		index = 0;
		val2 = 0;
		for (int i = 0; i < numRooms.length; i++) {
			index += numRooms[i].get() * (i + 1);
			val2 += numRooms[i].get();
		}
		val = (float) index / (float) val2;
		avgRooms.add(val);
	}

	public void findPercentile() {
		Collections.sort(avgRooms);
		index = (int) Math.ceil(.95 * avgRooms.size()) - 1;
	}

	@Override
	public String toString() {
		return "State "
				+ stateWithMostOld.toString()
				+ " records the highest percentage of elderly people."
				+ "\nIn the US, 95% of the states have an average number of rooms per house of "
				+ avgRooms.get(index) + " or less.";
	}

}
