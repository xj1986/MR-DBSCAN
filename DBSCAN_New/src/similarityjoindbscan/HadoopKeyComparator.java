package similarityjoindbscan;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import similarityjoindataformat.KeyWritable2;

public class HadoopKeyComparator extends WritableComparator {
	
	public HadoopKeyComparator() {
		super(KeyWritable2.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		KeyWritable2 k1 = (KeyWritable2) a;
		KeyWritable2 k2 = (KeyWritable2) b;
		
		int result = 0;
    	for(int i=0; i<k1.arrayList.size(); i++)
    	{
    		result = k1.arrayList.get(i).compareTo(k2.arrayList.get(i));
    		if (result != 0){
				return result>0?1:-1;
			}
    	}
    	
    	return (new Integer(k1.bitcode)).compareTo(new Integer(k2.bitcode));
	}

}
