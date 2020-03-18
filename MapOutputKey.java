package wise2012;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/////// CUSTOM MAP OUTPUT - REDUCE INPUT (KEY) (Method 2)
public class MapOutputKey implements WritableComparable<MapOutputKey> {
	private Text author;
	private int paperID;

	// Constructors
	public MapOutputKey() {
		set(new Text(), 0);
	}

	public MapOutputKey(String a, int pid) {
		set(new Text(a), pid);
	}

	public MapOutputKey(Text a, int pid) {
		set(a, pid);
	}

	// Accessors
	public Text get_author() {
		return author;
	}

	public int get_paperID() {
		return paperID;
	}

	// Mutator
	public void set(Text a, int pid) {
		author = a;
		paperID = pid;
	}

	// Wrapper determining how the object data are written out (from the Mapper)
	@Override public void write(DataOutput out) throws IOException {
		author.write(out);
		out.writeInt(paperID);
	}

	// Wrapper determining how the object data are read (from the Reducer)
	@Override public void readFields(DataInput in) throws IOException {
		author.readFields(in);
		paperID = in.readInt();
	}

	// Function determining hout the intermediate data are distributed to the Reducers
	@Override public int hashCode() {
		return author.hashCode() * 163 + Integer.toString(paperID).hashCode();
	}

	@Override public boolean equals(Object o) {
		if (o instanceof MapOutputKey) {
			MapOutputKey mok = (MapOutputKey) o;
			return author.equals(mok.author) && (paperID == mok.paperID);
		}
		return false;
	}

	@Override public String toString() {
		return author + "\t" + Integer.toString(paperID);
	}

	@Override public int compareTo (MapOutputKey mok) {
		int cmp = author.compareTo(mok.author);
		if (cmp != 0) {
			return cmp;
		}
		return paperID - mok.paperID;
	}
}
