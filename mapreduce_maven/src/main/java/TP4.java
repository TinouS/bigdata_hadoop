import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP4 {

	public static class MonProgMapper extends
			Mapper<IntWritable, Point2DWritable, IntWritable, Point2DWritable> {
		public void map(IntWritable key, Point2DWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class MonProgReducer extends
			Reducer<IntWritable, Point2DWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Point2DWritable> values,
				Context context) throws IOException, InterruptedException {
			String result = null;
			for (Point2DWritable point2dWritable : values) {
				result = point2dWritable.toString();
			}
			context.write(key, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("numberOfMappers", args[1]);
		conf.set("numberOfGenPoints", args[2]);
		Job job = Job.getInstance(conf, "TP4");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP4.class);
		job.setMapperClass(MonProgMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Point2DWritable.class);
		job.setReducerClass(MonProgReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(RandomPointInputFormat.class);
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}