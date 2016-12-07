import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
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
    static ArrayList<Centroid> centroidList = new ArrayList<Centroid>();
    static ArrayList<Centroid> newCentroidList = new ArrayList<Centroid>();
  public static class KMeansMapper
       extends Mapper<Object, Text, IntWritable, Text>{
          public void setup(Context context){
              
          }
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
              String tokens[] = value.toString().split(",");
              int col = Integer.parseInt(context.getConfiguration().get("col"));
              int var = Integer.parseInt(tokens[col]);
              int closest = 0;
              for (int i = 0; i<centroidList.size();i++){
                  if (Math.abs(centroidList.get(closest).returnValue(0)-var) > Math.abs(centroidList.get(i).returnValue(0)-var))
                      closest = i;
              }
              context.write(new IntWritable(closest), value);
		  //context.write(word, one);
	  }
  }
  
  public static class KMeansCombiner extends
			Reducer<IntWritable, Text, IntWritable, Text> {
      public void reduce(IntWritable key, Iterable<Text> values,
              Context context){
          
      }
  }
  
  public static class KMeansReducer
       extends Reducer<IntWritable ,Text ,NullWritable ,Text> {
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        int col = Integer.parseInt(context.getConfiguration().get("col"));
        int mean;
        int sum = 0, count = 0;
        for (Text value : values){
            count ++;
            String[] tokens = value.toString().split(",");
            sum += Integer.parseInt(tokens[col]);
            context.write(NullWritable.get(), new Text(value.toString()+","+key.get()));
        }
        mean = sum/count;
        newCentroidList.add(key.get(), new Centroid(null, mean));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("col", args[2]);
    Job job = Job.getInstance(conf, "KMeans");
    job.setNumReduceTasks(1);
    job.setJarByClass(KMeans.class);
    job.setMapperClass(KMeansMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    //job.setCombinerClass(KMeansCombiner.class);
    job.setReducerClass(KMeansReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(getOutputPath(args[0])));
    //generer une liste de centres en piochant dans le fichier d'input
    FileReader reader = new FileReader(new File(args[0]));
    BufferedReader bt = new BufferedReader(reader);
    String chaine;
    for (int i=0; i< Integer.parseInt(args[1]) && ((chaine=bt.readLine())!=null);i++){
        centroidList.add(new Centroid(chaine, Integer.parseInt(args[2])));
    }
    job.waitForCompletion(true);
    while(!centroidList.equals(newCentroidList)){
        centroidList = newCentroidList;
        File outputFile = new File(getOutputPath(args[0]));
        outputFile.delete();
        job.waitForCompletion(true);
    }
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
  public static String getOutputPath(String inputPath){
     String[] tokens = inputPath.split(".");
     String outPutPath = "";
     for (int i=0; i < tokens.length; i++){
         if (i == tokens.length-1){
             outPutPath += "_means_k."+ tokens[i];
         }
         else{
             outPutPath += tokens[i];
         }
     }
     return outPutPath; 
  }
}
