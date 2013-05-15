/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ccpreprocessing1;
import connectedcomponentsdataformat.PreEdgeKeySource;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;




public class PreCoreKeyComparator extends WritableComparator {

    protected PreCoreKeyComparator() {
        super(PreEdgeKeySource.class, true);
    }

    // compare based on both source and destination vertices
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        PreEdgeKeySource key01 = (PreEdgeKeySource) o1;
        PreEdgeKeySource key02 = (PreEdgeKeySource) o2;
        
        int cmp= compareSource(key01.source, key02.source);
        if (cmp!=0){
            return cmp;
        }
        
        return compareSource(key01.destination, key02.destination);

    }
    
    private int compareSource(int src1, int src2) {
        if (src1 > src2) {
            return 1;
        } else if (src1 < src2) {
            return -1;
        } else {
            return 0;
        }
    }
}