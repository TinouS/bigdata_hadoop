import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
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


//problème d'execution à partir de la seconde execution (le test pour lancer la boucle de job ne fonctionne pas comme prévu également)
public class KMeans {
    static ArrayList<Centroid> centroidList = new ArrayList<Centroid>();
    static ArrayList<Centroid> newCentroidList = new ArrayList<Centroid>();
  public static class KMeansMapper
       extends Mapper<Object, Text, IntWritable, Text>{
      static ArrayList<Centroid> cachedCentroidList = new ArrayList<Centroid>();
          public void setup(Context context) throws IOException{//extracting the data from the distributed cache
              int col = Integer.parseInt(context.getConfiguration().get("col"));
            URI[] files = context.getCacheFiles();
            DataInputStream istr = new DataInputStream(new FileInputStream(files[0].getPath()));
              BufferedReader strm = new BufferedReader(new InputStreamReader(istr));
              String chaine;
              for (int i=0;((chaine=strm.readLine())!=null);i++){
                  context.getCounter("clusters", "clusters read").increment(1);
                  if(col == 0)
                  context.getCounter("clusters", "cluster value "+i).increment(Integer.parseInt(chaine));
                cachedCentroidList.add(new Centroid(chaine, col));
                if (col != 0)
                    context.getCounter("clusters", "cluster value "+i).increment(cachedCentroidList.get(i).returnValue(col));
              }
              strm.close();
          }
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
              String tokens[] = value.toString().split(",");
              int col = Integer.parseInt(context.getConfiguration().get("col"));
              try{
              int var = Integer.parseInt(tokens[col]);
              int closest = 0;
              for (int i = 0; i<cachedCentroidList.size();i++){
                if (Math.abs(cachedCentroidList.get(closest).returnValue(0)-var) > Math.abs(cachedCentroidList.get(i).returnValue(0)-var))
                  closest = i;
                
              }
              context.getCounter("clusters", "cluster"+closest).increment(1);
              context.write(new IntWritable(closest), value);
              }catch(Exception e){
                  
              }
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
            //context.write(NullWritable.get(), new Text(value.toString()+","+key.get()));
        }
        mean = sum/count;
        context.write(NullWritable.get(), new Text(""+mean));
        //newCentroidList.add(new Centroid(mean));
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
    //job.setCombinerClass(KMeansCombiner.class);
    job.setReducerClass(KMeansReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //generer une liste de centres en piochant dans le fichier d'input
    FileSystem fs = FileSystem.get(conf);
    BufferedReader bt = new BufferedReader(new InputStreamReader(fs.open(new Path(args[0]))));
    OutputStream os = fs.create(new Path("clusters.txt"));
    System.out.println("clusters.txt created");
    //BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("clusters.txt"))));
    String chaine;
    for (int i=0; i< Integer.parseInt(args[2]) && ((chaine=bt.readLine())!=null);i++){
        centroidList.add(new Centroid(chaine, Integer.parseInt(args[3])));
        System.out.println("element"+chaine);
        //bw.write(chaine);
        os.write(chaine.getBytes());
        os.write("\n".getBytes());
    }
    //bw.close();
    os.close();
    job.addCacheFile(new Path("clusters.txt").toUri());
    job.waitForCompletion(true);
    fs.delete(new Path("clusters.txt"), true);
    //bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("clusters.txt"))));
    BufferedReader outputReader = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1]+"/part-r-00000"))));
    os = fs.create(new Path("clusters.txt"));
    while((chaine = outputReader.readLine()) != null){
        newCentroidList.add(new Centroid(chaine, 0));
        os.write(chaine.getBytes());
        os.write("\n".getBytes());
        //bw.write(chaine);
        //bw.newLine();
    }
    os.close();
    while(!compareClusters(centroidList, newCentroidList)){
        System.out.println("going in jobloop");
        fs.delete(new Path(args[1]), true);
        //File outputFile = new File(args[1]);
        //outputFile.delete();
        conf = new Configuration();
    conf.set("col", "0");
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
        System.out.println("another job done!!!!!!");
        System.out.println(centroidList.size());
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
  
  public static Boolean compareClusters(ArrayList<Centroid> oldlist, ArrayList<Centroid> newlist){
      return oldlist.equals(newlist);
  }
}
