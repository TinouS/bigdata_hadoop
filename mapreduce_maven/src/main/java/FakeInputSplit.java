import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

	public class FakeInputSplit extends InputSplit implements Writable{

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 1;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			String[] tab = {};
			return tab;
		}

		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}