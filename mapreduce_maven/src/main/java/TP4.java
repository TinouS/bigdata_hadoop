import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP4 {

	public static class MonProgMapper extends
			Mapper<IntWritable, Point2DWritable, NullWritable, BooleanWritable> {
		public void map(IntWritable key, Point2DWritable value, Context context)
				throws IOException, InterruptedException {
                    double x = value.getX();
                    double y = value.getY();
                    if((Math.pow(x, 2)+Math.pow(y, 2))<1){
                        context.write(NullWritable.get(), new BooleanWritable(true));
                    }
			context.write(NullWritable.get(), new BooleanWritable(false));
		}
	}

	public static class MonProgReducer extends
			Reducer<NullWritable, BooleanWritable, NullWritable, DoubleWritable> {
		public void reduce(NullWritable key, Iterable<BooleanWritable> values,
				Context context) throws IOException, InterruptedException {
                        int counterIn = 0;
                        int counterTotal = 0;
			for (BooleanWritable isInCircle : values) {
                            if (isInCircle.get())
                                counterIn++;
                            counterTotal++;
			}
			context.write(key, new DoubleWritable(4*(counterTotal/counterIn)));
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
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BooleanWritable.class);
		job.setReducerClass(MonProgReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(RandomPointInputFormat.class);
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}