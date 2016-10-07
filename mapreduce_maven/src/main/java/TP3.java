import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
	public static class MyWritable implements Writable {
		int min, max, counter, sum;

		MyWritable() {
			this.min = -1;
			this.max = -1;
			this.counter = 0;
			this.sum = 0;
		}

		MyWritable(int min, int max, int sum, int counter) {
			this.min = min;
			this.max = max;
			this.sum = sum;
			this.counter = counter;
		}

		public void write(DataOutput d) throws IOException {
			d.writeInt(min);
			d.writeInt(max);
			d.writeInt(counter);
			d.writeInt(sum);
		}

		public void readFields(DataInput di) throws IOException {
			min = di.readInt();
			max = di.readInt();
			counter = di.readInt();
			sum = di.readInt();
		}
	}

	public static class TP3Mapper extends
			Mapper<Object, Text, IntWritable, MyWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Counter cnt = context.getCounter("WCP", "nb_cities");
			cnt.increment(1);

			Counter cnt2 = context.getCounter("WCP", "nb_pop");

			Counter cnt3 = context.getCounter("WCP", "total_pop");

			String tokens[] = value.toString().split(",");
			if (!tokens[4].isEmpty()) {
				cnt2.increment(1);
				if (!tokens[4].equals("Population")) {
					cnt3.increment(Integer.parseInt(tokens[4]));

					// LOG WITH ARGUMENT "base"
					double bLog = Double.parseDouble(context.getConfiguration()
							.get("base"));
					int logResult = (int) (Math.log(Double
							.parseDouble(tokens[4])) / Math.log(bLog));
                    // LOG 10
					
					Counter tmpCnt = context.getCounter("Hist", String
							.valueOf((int) Math.log10(Double
									.parseDouble(tokens[4]))));
					tmpCnt.increment(1);
					context.write(
							new IntWritable((int) Math.log10(Double
									.parseDouble(tokens[4]))), new MyWritable(
									-1, -1, Integer.parseInt(tokens[4]), -1));
				}
			}
		}
	}

	public static class TP3Combiner extends
			Reducer<IntWritable, MyWritable, IntWritable, MyWritable> {
		public void reduce(IntWritable key, Iterable<MyWritable> values,
				Context context) throws IOException, InterruptedException {
			int min = -1, max = -1, sum = 0;
			int counter = 0;
			for (MyWritable val : values) {
				if (val.sum < min | min == -1)
					min = val.sum;
				if (val.sum > max)
					max = val.sum;
				sum += val.sum;
				counter++;
			}
			context.write(key, new MyWritable(min, max, sum, counter));
		}
	}

	public static class TP3Reducer extends
			Reducer<IntWritable, MyWritable, DoubleWritable, Text> {
		public void reduce(IntWritable key, Iterable<MyWritable> values,
				Context context) throws IOException, InterruptedException {
			int counter = 0;
			int sum = 0;
			int max = -1;
			int min = -1;
			for (MyWritable value : values) {
				if (value.max > max)
					max = value.max;
				if (value.min < min | min == -1)
					min = value.min;
				sum += value.sum;
				counter += value.counter;
			}
			int avg = sum / counter;
			DoubleWritable newkey = new DoubleWritable(Math.pow(10, key.get()));
			String result = counter + " " + avg + " " + max + " " + min;
			context.write(newkey, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("base", args[2]);
		Job job = Job.getInstance(conf, "TP3");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP3.class);
		job.setMapperClass(TP3Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MyWritable.class);
		job.setCombinerClass(TP3Combiner.class);
		job.setReducerClass(TP3Reducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
