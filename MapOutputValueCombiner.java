package wise2012;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Writable;

///////// CUSTOM MAP OUTPUT - REDUCE INPUT (VALUE) (Method 1-C)
public class MapOutputValueCombiner implements Writable {
	private int num_values;
	private Vector<MapOutputValue> MapOutputValuesVector;

	// No-argument (default) object constructor
	public MapOutputValueCombiner() {
		this.num_values = 0;
		this.MapOutputValuesVector = new Vector<MapOutputValue>();
	}

	// Constructor with arguments (data for the first Vector record)
	public MapOutputValueCombiner(int id, float score) {
		this.MapOutputValuesVector = new Vector<MapOutputValue>();
		insert(id, score);
	}

	// Wrapper determining how the object data are written out (from the Mapper)
	public void write(DataOutput out) throws IOException {

		// Flush the number of vector elements
		out.writeInt(this.num_values);

		// Now write each vector value in the output buffer
		for (int i = 0; i < this.num_values; i++) {
			if (this.MapOutputValuesVector.elementAt(i) instanceof MapOutputValue) {
				out.writeInt(this.MapOutputValuesVector.elementAt(i).get_paperID());
				out.writeFloat(this.MapOutputValuesVector.elementAt(i).get_paperScore());
			}
		}
	}

	// Wrapper determining how the object data are read (from the Reducer)
	public void readFields(DataInput in) throws IOException {
		// How many values to read?
		this.num_values = in.readInt();

		// Read each value
		for (int i = 0; i < this.num_values; i++) {
			MapOutputValue temp = new MapOutputValue(in.readInt(), in.readFloat());
			this.MapOutputValuesVector.add(temp);
		}
	}

	public String toString() {
		String temp = new String();
		for (int i = 0; i < this.num_values; i++) {
			if (this.MapOutputValuesVector.elementAt(i) instanceof MapOutputValue) {
				temp += this.MapOutputValuesVector.elementAt(i).toString();
			}
		}
		return temp;
	}

	// Accessors _get: Return private object data
	public int get_num_values() {
		return this.num_values;
	}

	public Vector<MapOutputValue> get_values() {
		return this.MapOutputValuesVector;
	}

	// Mutator: Insert a MapOuputValue record into the Vector
	public void insert(int id, float score) {
		// Search: the incoming id may already be present in the Vector. In this case add the
		// incoming score to the existing one. This is a sequential (and possibly slow) search.
		for (int i = 0; i < this.num_values; i++) {
			if (this.MapOutputValuesVector.elementAt(i) instanceof MapOutputValue) {
				if (this.MapOutputValuesVector.elementAt(i).get_paperID() == id) {
					MapOutputValue temp = new MapOutputValue(id,
							score + this.MapOutputValuesVector.get(i).get_paperScore());
					this.MapOutputValuesVector.set(i, temp);
					return;
				}
			}
		}

		// If the incoming id was not found, create a new record and place it in the Vector's tail
		MapOutputValue temp = new MapOutputValue(id, score);
		this.MapOutputValuesVector.add(temp);
		this.num_values++;
	}
}
