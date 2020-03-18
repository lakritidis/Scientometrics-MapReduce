package wise2012;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Writable;

public class MapOutputValueCombiner_2C implements Writable {
	private int num_values;
	private Vector<Float> values;

	// No-argument (default) object constructor
	public MapOutputValueCombiner_2C() {
		this.num_values = 0;
		this.values = new Vector<Float>();
	}

	// Constructor with arguments
	public MapOutputValueCombiner_2C(float val) {
		this.num_values = 0;
		this.values = new Vector<Float>();
		insert(val);
	}

	// Wrapper determining how the object data are read (from the Reducer)
	public void readFields(DataInput in) throws IOException {
		this.num_values = in.readInt();
		for (int i = 0; i < this.num_values; i++) {
			this.values.add(in.readFloat());
		}
	}

	// Wrapper determining how the object data are written out (from the Mapper)
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.num_values);
		for (int i = 0 ; i < this.num_values; i++) {
			out.writeFloat(this.values.elementAt(i).floatValue());	
		}
	}

	// Mutator: Insert a float value (paper score) into the Vector
	public void insert(float val) {
		this.values.add(val);
	}
}
