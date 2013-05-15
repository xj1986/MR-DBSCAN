package similarityjoindataformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;


public class KeyWritable implements WritableComparable<KeyWritable> {

	public ArrayList<Integer> arrayList;
//	int bitcode;
	
	public KeyWritable (){
		arrayList = new ArrayList<Integer>();
//		bitcode = 0;
	}

	public KeyWritable (ArrayList<Integer> al){
		this.arrayList = new ArrayList<Integer>();
		for(Integer i : al){
			this.arrayList.add(new Integer(i.intValue()));
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
//		out.writeInt(bitcode);
		out.writeInt(arrayList.size());
		for (Integer i: arrayList)
			out.writeInt(i);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
//		bitcode= in.readInt();
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

		return s;
	}
	
	public int hash() {
		return arrayList.hashCode();
	}

	@Override
	public int compareTo(KeyWritable arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
}
