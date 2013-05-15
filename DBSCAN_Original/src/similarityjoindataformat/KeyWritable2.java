package similarityjoindataformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;


public class KeyWritable2 implements WritableComparable<KeyWritable2> {

	public ArrayList<Integer> arrayList;
	public int bitcode;
	
	public KeyWritable2 (){
		arrayList = new ArrayList<Integer>();
		bitcode = 0;
	}

	public KeyWritable2 (ArrayList<Integer> al, int bi){
		this.arrayList = new ArrayList<Integer>();
		for(Integer i : al){
			this.arrayList.add(new Integer(i.intValue()));
		}
		this.bitcode=bi;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(bitcode);
		out.writeInt(arrayList.size());
		for (Integer i: arrayList)
			out.writeInt(i);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		bitcode= in.readInt();
		arrayList = new ArrayList<Integer>();
		try {
			int s = in.readInt();
			for (int i = 0; i < s; i++) 
				this.arrayList.add(in.readInt()); // readInt mechanism: actually, it is reading a Input stream step by step.
		} catch (Exception e) {
			// TODO: handle exception
		}
}
	public String toString() { 
		String s = (arrayList.size()==0)?"":Integer.toString(arrayList.get(0));

		for (int i = 1; i < arrayList.size(); i++) {
			s += (", "+Integer.toString(arrayList.get(i)));
		}
		s += "|"+ Integer.toString(bitcode);

		return s;
	}
	
	public int hash() {
		return arrayList.hashCode();
	}

	@Override
	public int compareTo(KeyWritable2 o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
