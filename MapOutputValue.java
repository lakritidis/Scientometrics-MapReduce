package wise2012;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


///////// CUSTOM MAP OUTPUT - REDUCE INPUT (VALUE) (Methods 1, 1-C)
public class MapOutputValue implements Writable {
	private int paperID;
	private float paperScore;

	// No-argument (default) object constructor
	public MapOutputValue() {
		this(0, 0.0f);
	}

	// Constructor with arguments
	public MapOutputValue(int id, float score) {
		this.paperID = id;
	    this.paperScore = score;
	}

	// Wrapper determining how the object data are written out (from the Mapper)
	public void write(DataOutput out) throws IOException {
		out.writeInt(paperID);
		out.writeFloat(paperScore);
	}

	// Wrapper determining how the object data are read (from the Reducer)
	public void readFields(DataInput in) throws IOException {
		paperID = in.readInt();
		paperScore = in.readFloat();
	}

	public String toString() {
		return Integer.toString(paperID) + ", " + Float.toString(paperScore) + ". ";
	}

	// Accessors
	public int get_paperID() {
		return this.paperID;
	}

	public float get_paperScore() {
		return this.paperScore;
	}
}