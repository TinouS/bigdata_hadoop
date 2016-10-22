import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {

	public static class MonProgMapper extends
			Mapper<Object, Text, NullWritable, VilleWritable> {

		public int k = 0;
		private TreeMap<Float, VilleWritable> topKVilles = new TreeMap<Float, VilleWritable>();

		public void setup(Context context) {
			k = context.getConfiguration().getInt("kValue", 10);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			if (!(tokens[4].isEmpty()) & !tokens[4].equals("Population")) {
				topKVilles.put(Float.parseFloat(tokens[4]), new VilleWritable(
						Float.parseFloat(tokens[4]), value.toString()));
			}
			if (topKVilles.size() > k) {
				topKVilles.remove(topKVilles.firstKey());
			}
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (VilleWritable value : topKVilles.values()) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static class MonProgCombiner extends
			Reducer<NullWritable, VilleWritable, NullWritable, VilleWritable> {
		public int k = 0;
		private TreeMap<Float, VilleWritable> topKVilles = new TreeMap<Float, VilleWritable>();

		public void setup(Context context) {
			k = context.getConfiguration().getInt("kValue", 10);
		}

		public void reduce(NullWritable key, Iterable<VilleWritable> values,
				Context context) throws IOException, InterruptedException {
			for (VilleWritable value : values) {
				topKVilles.put(value.getPopulation(), value.clone());
				if (topKVilles.size() > k) {
					topKVilles.remove(topKVilles.firstKey());
				}
			}
			for (VilleWritable value : topKVilles.values()) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static class MonProgReducer extends
			Reducer<NullWritable, VilleWritable, NullWritable, Text> {

<<<<<<< HEAD
        public void setup(Context context){
            k = context.getConfiguration().getInt("kValue", 10);
        }
        public void reduce(NullWritable key, Iterable<VilleWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            for (VilleWritable value : values) {
                topKVilles.put(value.getPopulation(), value.clone());
                if (topKVilles.size() > k) {
                    topKVilles.remove(topKVilles.firstKey());
                }
            }
            for (VilleWritable value : topKVilles.values()){
                context.write(NullWritable.get(), value);
            }
        }
    }
    
    public static class MonProgReducer
            extends Reducer<NullWritable, VilleWritable, NullWritable, Text> {
=======
		public int k = 0;
		private TreeMap<Float, String> topKVilles = new TreeMap<Float, String>();
>>>>>>> e3ba29d9c20a4501b5f8a3a7ce1d5f01eb3b4116

		public void setup(Context context) {
			k = context.getConfiguration().getInt("kValue", 10);
		}

		public void reduce(NullWritable key, Iterable<VilleWritable> values,
				Context context) throws IOException, InterruptedException {
			for (VilleWritable value : values) {
				topKVilles.put(value.getPopulation(), value.getInfo());
				if (topKVilles.size() > k) {
					topKVilles.remove(topKVilles.firstKey());
				}
			}
			for (String value : topKVilles.descendingMap().values()) {
				context.write(NullWritable.get(), new Text(value));
			}
		}
	}

<<<<<<< HEAD
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("kValue", args[2]);
        Job job = Job.getInstance(conf, "MonProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(TP5.class);
        job.setMapperClass(MonProgMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(VilleWritable.class);
        job.setCombinerClass(MonProgCombiner.class);
        job.setReducerClass(MonProgReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
=======
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("kValue", args[2]);
		Job job = Job.getInstance(conf, "MonProg");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP5.class);
		job.setMapperClass(MonProgMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(VilleWritable.class);
		job.setCombinerClass(MonProgCombiner.class);
		job.setReducerClass(MonProgReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
>>>>>>> e3ba29d9c20a4501b5f8a3a7ce1d5f01eb3b4116
}
