import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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

public class KMeansHier {

	static ArrayList<ArrayList<Double>> centroidList = new ArrayList<ArrayList<Double>>();
	static ArrayList<ArrayList<Double>> newCentroidList = new ArrayList<ArrayList<Double>>();

	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

		static ArrayList<ArrayList<Double>> cachedCentroidList = new ArrayList<ArrayList<Double>>();

		public void setup(Context context) throws IOException {
			URI[] files = context.getCacheFiles();
			DataInputStream istr = new DataInputStream(new FileInputStream(files[0].getPath()));
			BufferedReader strm = new BufferedReader(new InputStreamReader(istr));
			String chaine;
			for (int i = 0; ((chaine = strm.readLine()) != null) && !chaine.isEmpty(); i++) {
				String tokens[] = chaine.split(",");
				cachedCentroidList.add(new ArrayList<Double>());
				for (String token : tokens) {
					cachedCentroidList.get(i).add(Double.parseDouble(token));
				}
			}
			strm.close();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split(",");
			List<Double> point = new ArrayList<Double>();
			try {
				for (int i = 0; i < Double.parseDouble(context.getConfiguration().get("dim")); i++) {
					point.add(Double.parseDouble(tokens[Integer.parseInt(context.getConfiguration().get("col " + i))]));
				}
				int closest = 0;
				for (int i = 0; i < cachedCentroidList.size(); i++) {
					if (getDistance(point, cachedCentroidList.get(closest)) > getDistance(point,
							cachedCentroidList.get(i))) {
						closest = i;
					}
				}

				context.write(new IntWritable(closest), value);
			} catch (Exception e) {

			}
		}

		public static double getDistance(List<Double> centroid, List<Double> point) {
			if (centroid.size() == point.size()) {
				double sum = 0;
				for (int i = 0; i < centroid.size(); i++) {
					sum += Math.pow(Math.abs(centroid.get(i) - point.get(i)), 2);
				}
				return Math.sqrt(sum);
			}
			return 0;
		}
	}

	public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) {

		}
	}

	public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<Double> sums = new ArrayList<Double>();
			int count = 0;
			for (int i = 0; i < Integer.parseInt(context.getConfiguration().get("dim")); i++) {
				sums.add(0.0);
			}

			for (Text value : values) {
				count++;
				String[] tokens = value.toString().split(",");
				for (int i = 0; i < Integer.parseInt(context.getConfiguration().get("dim")); i++) {
					double tmp = sums.get(i);
					sums.set(i, tmp += Double
							.parseDouble(tokens[Integer.parseInt(context.getConfiguration().get("col " + i))]));
				}
			}

			List<Double> means = new ArrayList<Double>();
			String returnString = "";
			for (int i = 0; i < Integer.parseInt(context.getConfiguration().get("dim")); i++) {
				means.add(sums.get(i) / count);
				if (i == Integer.parseInt(context.getConfiguration().get("dim")) - 1) {
					returnString += (sums.get(i) / count);
				} else {
					returnString += (sums.get(i) / count) + ",";
				}
			}
			context.write(NullWritable.get(), new Text(returnString));
		}
	}

	public static void main(String[] args) throws Exception {
		run(args, 0);
	}

	public static void run(String[] args, int iteration) throws Exception {
		int dim = args.length - 4;
		int wantedIterations = Integer.parseInt(args[3]);
		List<Integer> cols = new ArrayList<Integer>();
		for (int i = 4; i < args.length; i++) {
			cols.add(i - 4, Integer.parseInt(args[i]));
		}
		Configuration conf = new Configuration();
		for (int value : cols) {
			conf.set("col " + cols.indexOf(value), "" + value);
		}
		conf.set("dim", "" + dim);

		Job job = Job.getInstance(conf, "KMeansHier");
		job.setNumReduceTasks(1);
		job.setJarByClass(KMeansHier.class);
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
		// generer une liste de centres en piochant dans le fichier d'input
		FileSystem fs = FileSystem.get(conf);
		BufferedReader bt = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0]))));
		OutputStream os = fs.create(new Path("clusters.txt"));

		String chaine;
		ArrayList<Double> previouscentroid = null;
		for (int i = 0; i < Integer.parseInt(args[2]) && ((chaine = bt.readLine()) != null); i++) {
			try {
				ArrayList<Double> centroidCandidate = new ArrayList<Double>();

				String[] tokens = chaine.split(",");

				for (int j = 4; j < args.length; j++) {
					centroidCandidate.add(Double.parseDouble(tokens[Integer.parseInt(args[j])]));
				}

				if (!centroidList.contains(centroidCandidate)) {
					centroidList.add(centroidCandidate);
					int counter = 0;
					for (Double value : centroidCandidate) {
						if (counter != centroidCandidate.size() - 1) {
							os.write((value + ",").getBytes());
							counter++;
						} else {
							os.write((value + "").getBytes());
						}
					}
					os.write("\n".getBytes());

					previouscentroid = (ArrayList<Double>) centroidCandidate.clone();
				} else {
					i--;
				}

			} catch (Exception e) {
				i--;

			}
		}
		// bw.close();
		os.close();
		job.addCacheFile(new Path("clusters.txt").toUri());
		job.waitForCompletion(true);
		fs.delete(new Path("clusters.txt"), true);
		copyClusters(conf, args[1]);
		while (!compareClusters(centroidList, newCentroidList)) {

			fs.delete(new Path(args[1]), true);
			conf = new Configuration();
			for (int i = 4; i < args.length; i++) {
				conf.set("col " + (i - 4), args[i]);// Labels start from 0
			}
			conf.set("dim", "" + (args.length - 4));
			job = Job.getInstance(conf, "KMeansHier");
			job.addCacheFile(new Path("clusters.txt").toUri());
			job.setNumReduceTasks(1);
			job.setJarByClass(KMeansHier.class);
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
			centroidList = (ArrayList<ArrayList<Double>>) newCentroidList.clone();
			copyClusters(conf, args[1]);
		}

		fs.delete(new Path(args[1]), true);
		conf = new Configuration();
		for (int i = 3; i < args.length; i++) {
			conf.set("col " + (i - 3), args[i]);// Labels start from 0
		}
		conf.set("dim", "" + (args.length - 3));
		job = Job.getInstance(conf, "KMeansHier");
		job.addCacheFile(new Path("clusters.txt").toUri());
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setJarByClass(KMeansHier.class);
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

		if (iteration < wantedIterations) {
			String previousOutput = args[1];
			for (int i = 0; i < Integer.parseInt(args[2]); i++) {
				args[0] = previousOutput + "/part-r-0000" + i;
				args[1] = previousOutput + i;
				run(args, iteration + 1);
			}
			fs.delete(new Path(previousOutput), true);
		}
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

	public static Boolean compareClusters(ArrayList<ArrayList<Double>> oldlist, ArrayList<ArrayList<Double>> newlist) {// returns
																														// true
																														// if
																														// equal

		if (oldlist.size() != newlist.size()) {

			return false;
		}
		for (int i = 0; i < oldlist.size(); i++) {
			if (oldlist.get(i).size() != newlist.get(i).size()) {

				return false;
			}
			for (int j = 0; j < oldlist.get(i).size(); j++) {
				if (!oldlist.get(i).get(j).equals(newlist.get(i).get(j))) {

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
		newCentroidList = new ArrayList<ArrayList<Double>>();

		while ((chaine = outputReader.readLine()) != null) {
			newCentroidList.add(new ArrayList<Double>());
			String[] tokens = chaine.split(",");
			for (int j = 0; j < Double.parseDouble(conf.get("dim")); j++) {
				newCentroidList.get(i).add(Double.parseDouble(tokens[j]));
			}

			os.write(chaine.getBytes());
			os.write("\n".getBytes());
			i++;
		}
		outputReader.close();
		os.close();
	}

	public static void generateClusters(String[] args, BufferedReader bt, OutputStream os) throws IOException {
		String chaine;
		for (int i = 0; i < Double.parseDouble(args[2]) && ((chaine = bt.readLine()) != null); i++) {
			try {
				centroidList.add(new ArrayList<Double>());
				String[] tokens = chaine.split(",");

				for (int j = 3; j < args.length; j++) {
					centroidList.get(i).add(Double.parseDouble(tokens[Integer.parseInt(args[j])]));
					if (j == args.length - 1) {
						os.write(tokens[Integer.parseInt(args[j])].getBytes());
					} else {
						os.write((tokens[Integer.parseInt(args[j])] + ",").getBytes());
					}
				}
				os.write("\n".getBytes());
			} catch (Exception e) {
				i--;

			}
		}
	}
}
