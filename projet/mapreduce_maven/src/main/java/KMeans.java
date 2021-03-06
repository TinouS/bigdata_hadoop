import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {

	static ArrayList<ArrayList<Integer>> centroidList = new ArrayList<ArrayList<Integer>>();
	static ArrayList<ArrayList<Integer>> newCentroidList = new ArrayList<ArrayList<Integer>>();

	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

		static ArrayList<ArrayList<Integer>> cachedCentroidList = new ArrayList<ArrayList<Integer>>();

		public void setup(Context context) throws IOException {
			URI[] files = context.getCacheFiles();
			DataInputStream istr = new DataInputStream(new FileInputStream(files[0].getPath()));
			BufferedReader strm = new BufferedReader(new InputStreamReader(istr));
			String chaine;
			for (int i = 0; ((chaine = strm.readLine()) != null); i++) {
				cachedCentroidList.add(new ArrayList<Integer>());
				cachedCentroidList.get(i).add(Integer.parseInt(chaine));
			}
			strm.close();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			int col = Integer.parseInt(context.getConfiguration().get("col"));
			try {
				int var = Integer.parseInt(tokens[col]);
				int closest = 0;
				for (int i = 0; i < cachedCentroidList.size(); i++) {
					if (Math.abs(cachedCentroidList.get(closest).get(0) - var) > Math
							.abs(cachedCentroidList.get(i).get(0) - var)) {
						closest = i;
					}

				}
				context.write(new IntWritable(closest), value);
			} catch (Exception e) {
			}
		}
	}

	public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) {

		}
	}

	public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int col = Integer.parseInt(context.getConfiguration().get("col"));
			int mean;
			int sum = 0, count = 0;
			for (Text value : values) {
				count++;
				String[] tokens = value.toString().split(",");
				sum += Integer.parseInt(tokens[col]);
			}
			mean = sum / count;
			context.write(NullWritable.get(), new Text("" + mean));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("col", args[3]);
		Job job = Job.getInstance(conf, "KMeans");
		job.setNumReduceTasks(1);
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// job.setCombinerClass(KMeansCombiner.class);
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(conf);
		BufferedReader bt = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0]))));
		OutputStream os = fs.create(new Path("clusters.txt"));
		String chaine;
		for (int i = 0; i < Integer.parseInt(args[2]) && ((chaine = bt.readLine()) != null); i++) {
			centroidList.add(new ArrayList<Integer>());

			int col = Integer.parseInt(args[3]);
			String[] tokens = chaine.split(",");
			centroidList.get(i).add(Integer.parseInt(tokens[col]));

			os.write(tokens[col].getBytes());
			os.write("\n".getBytes());
		}
		// bw.close();
		os.close();
		job.addCacheFile(new Path("clusters.txt").toUri());
		job.waitForCompletion(true);
		fs.delete(new Path("clusters.txt"), true);
		copyClusters(conf, args[1]);
		while (!compareClusters(centroidList, newCentroidList)) {
			System.out.println("going in jobloop");
			fs.delete(new Path(args[1]), true);
			conf = new Configuration();
			conf.set("col", args[3]);
			job = Job.getInstance(conf, "KMeans");
			job.addCacheFile(new Path("clusters.txt").toUri());
			job.setNumReduceTasks(1);
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);

			fs.delete(new Path("clusters.txt"), true);
			centroidList = (ArrayList<ArrayList<Integer>>) newCentroidList.clone();
			copyClusters(conf, args[1]);
		}

		fs.delete(new Path(args[1]), true);
		conf = new Configuration();
		conf.set("col", args[3]);
		job = Job.getInstance(conf, "KMeansFinal");
		job.addCacheFile(new Path("clusters.txt").toUri());
		job.setNumReduceTasks(1);
		job.setJarByClass(KMeans.class);
		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(FinalKmeansReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return;
	}

	public static String getOutputPath(String inputPath) {
		String[] tokens = inputPath.split(".");
		String outPutPath = "";
		for (int i = 0; i < tokens.length; i++) {
			if (i == tokens.length - 1) {
				outPutPath += "_means_k." + tokens[i];
			} else {
				outPutPath += tokens[i];
			}
		}
		return outPutPath;
	}

	public static Boolean compareClusters(ArrayList<ArrayList<Integer>> oldlist,
			ArrayList<ArrayList<Integer>> newlist) {

		if (oldlist.size() != newlist.size()) {
			return false;
		}
		for (int i = 0; i < oldlist.size(); i++) {
			if (oldlist.get(i).size() != newlist.get(i).size()) {
				return false;
			}
			for (int j = 0; j < oldlist.get(i).size(); j++) {
				if (oldlist.get(i).get(j) != newlist.get(i).get(j)) {
					return false;
				}
			}
		}
		return true;

	}

	public static void copyClusters(Configuration conf, String outPutPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		OutputStream os = fs.create(new Path("clusters.txt"));
		BufferedReader outputReader = new BufferedReader(
				new InputStreamReader(fs.open(new Path(outPutPath + "/part-r-00000"))));
		String chaine;
		int i = 0;
		newCentroidList = new ArrayList<ArrayList<Integer>>();

		while ((chaine = outputReader.readLine()) != null) {
			newCentroidList.add(new ArrayList<Integer>());
			newCentroidList.get(i).add(Integer.parseInt(chaine));

			os.write(chaine.getBytes());
			os.write("\n".getBytes());
			i++;
		}
		outputReader.close();
		os.close();
	}
}
