import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends
		RecordReader<IntWritable, Point2DWritable> {
	int counter = 0;
	int max = 0;
	Point2DWritable point2D;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		max = Integer.parseInt(context.getConfiguration().get(
				"numberOfGenPoints"));
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (counter < max) {
			Double v1 = (Double) Math.random();
			Double v2 = (Double) Math.random();
			point2D = new Point2DWritable(v1, v2);
			counter++;
			return true;
		} else
			return false;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return new IntWritable(counter);
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException,
			InterruptedException {
		return point2D;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}
}