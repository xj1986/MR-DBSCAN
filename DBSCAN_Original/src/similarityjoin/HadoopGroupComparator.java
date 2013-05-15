package similarityjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import similarityjoindataformat.KeyWritable2;

public class HadoopGroupComparator extends WritableComparator {

	public HadoopGroupComparator() {
		super(KeyWritable2.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		KeyWritable2 k1 = (KeyWritable2) a;
		KeyWritable2 k2 = (KeyWritable2) b;
//		return k1.arrayList.compareTo(k2.arrayList);
		
		int result = 0;
    	for(int i=0; i<k1.arrayList.size(); i++)
    	{
    		result = k1.arrayList.get(i).compareTo(k2.arrayList.get(i));
    		if (result != 0){
				return result>0?1:-1;
			}
    	}
    	return 0;
		
	}

}
