import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

	public class Point2DWritable implements Writable {

		public double x;
		public double y;
		
		
		public Point2DWritable() {
		}

		public Point2DWritable(double x, double y) {
			
			this.x=x;
			this.y=y;
			
		}

		public void write(DataOutput out) throws IOException {
			out.writeDouble(this.getX());
			out.writeDouble(this.getY());
		}

		public void readFields(DataInput in) throws IOException {
			x = in.readDouble();
			y = in.readDouble();
		}

		public double getX() {
			return x;
		}

		public void setX(double x) {
			this.x = x;
		}

		public double getY() {
			return y;
		}

		public void setY(double y) {
			this.y = y;
		}

		@Override
		public String toString() {
			return this.getX() + " | " + this.getY();
		}

	}
