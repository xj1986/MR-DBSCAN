package similarityjoindataformat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class ValueWritable implements Writable {

	public int key;
	public ArrayList<Double> arrayList;
	public int bitcode;
	
	public ValueWritable(){
		key=0;
		arrayList = new ArrayList<Double>();
		bitcode = 0;
	}
	
	public ValueWritable(int k, ArrayList<Double> al, int bc){
		key=k;
		arrayList=new ArrayList<Double>();
		for(Double d : al){
			this.arrayList.add(new Double(d.doubleValue()));
		}
		bitcode=bc;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(key);
		out.writeInt(bitcode);
		out.writeInt(arrayList.size());
		for (Double d: arrayList)
		out.writeDouble(d);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		key=in.readInt();
		bitcode=in.readInt();
		int arrayListSize = in.readInt();
		arrayList = new ArrayList<Double>();
		
		for (int i=0; i<arrayListSize; i++){
			arrayList.add(in.readDouble());
		}
	}
	
	public String toString() { 

		
	
			String s = Integer.toString(key)+"|";
			s += (arrayList.size()==0)?"":Double.toString(arrayList.get(0));

			for (int i = 1; i < arrayList.size(); i++) {
				s += (", "+Double.toString(arrayList.get(i)));
				
			}
			
			s += "|"+ Integer.toString(bitcode);

			return s;
		}

}
