package connectedcomponentsdataformat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class EdgeKey implements WritableComparable<EdgeKey> {

    public int source, destination;

    public EdgeKey() {
    }

    public EdgeKey(int source, int destination) {
        this.source = source;
        this.destination = destination;
    }

    public void readFields(DataInput in) throws IOException {
        source = in.readInt();
        destination = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(source);
        out.writeInt(destination);
    }

    @Override
    public String toString() {
        return (String.valueOf(source) + "," + String.valueOf(destination));
    }

    @Override
    public boolean equals(Object other) {
        EdgeKey obj = (EdgeKey) other;
        if (source == obj.source && destination == obj.destination) {
            return true;
        }
        return false;
    }


    /*
     * This method sorts based on the two vertices except that it moves the
     * vertic with negative destination (non-core object) to the end of the sorted
     * list. This is done to avoid keeping such points in reducer'e memory.
     */
    public int compareTo(EdgeKey t) {

        if (source > t.source) {
            return 1;
        } else if (source < t.source) {
            return -1;
        } else {

            // take care of noncore points sort such that it considers them inf
            // when comparing with core objects
            if (destination < 0 && t.destination < 0) {
                if (destination > t.destination) {
                    return -1;
                } else {
                    return 1;
                }
            } else {
                if (t.destination < 0) {
                    return -1;
                }

                if (destination < 0) {
                    return 1;
                }

            }
            if (destination > t.destination) {
                return 1;
            } else if (destination < t.destination) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
