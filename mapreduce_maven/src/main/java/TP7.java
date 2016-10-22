import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP7 {
  public static class MapperTown
       extends Mapper<Object, Text, TaggedKey, TaggedValue>{
      public void map(Object key, Text value, Context context
              ) throws IOException, InterruptedException {
          String [] tokens = value.toString().split(",");
          String returnKey = tokens[0].toUpperCase() + "," + tokens[3];
          String returnValue = tokens[2];
          context.write(new TaggedKey(0, returnKey), new TaggedValue(0, returnValue));
      }
  }
  
   public static class MapperRegion
       extends Mapper<Object, Text, TaggedKey, TaggedValue>{
      public void map(Object key, Text value, Context context
              ) throws IOException, InterruptedException {
          String [] tokens = value.toString().split(",");
          String returnKey = tokens[0] + "," + tokens[1];
          String returnValue = tokens[2];
          context.write(new TaggedKey(1,returnKey), new TaggedValue(1, returnValue));
      }
  } 
   
  public static class MonProgPartitioner extends Partitioner<TaggedKey, TaggedValue>{
      
        @Override
        public int getPartition(TaggedKey key, TaggedValue value, int i) {
            String naturalKey = key.getNaturalKey();
            return naturalKey.hashCode()%i;
        }
      
  }
  
  public static class MonProgGrouper extends WritableComparator{
      
      public MonProgGrouper (){
          super(TaggedKey.class, true);
      }
      
      public int compare(TaggedKey a, TaggedKey b){
          return a.getNaturalKey().compareTo(b.getNaturalKey());
      }
  }
  
  public static class MonProgSorter extends WritableComparator{
      
      public MonProgSorter(){
          super(TaggedKey.class, true);
      }
      
      public int compare(TaggedKey a, TaggedKey b){
          return (a.getType() < b.getType()) ? 1 : -1;
      }
  }
  
  public static class MonProgReducer
       extends Reducer<TaggedKey,TaggedValue,NullWritable,Text> {
      String region = null;
    public void reduce(TaggedKey key, Iterable<TaggedValue> values,
                       Context context
                       ) throws IOException, InterruptedException {
        
        for(TaggedValue value : values){
            if (value.getType() == 1)
                region = value.getValue();
            else
                context.write(NullWritable.get(), new Text(value.getValue()+ "," + region));
        }
        /*ArrayList<String> tmpTown = new ArrayList<String>();
        String region = null;
        for(TaggedValue value : values){
            if (region == null && value.getType() == 0){
                tmpTown.add(value.getValue());
                continue;
            }
            if (value.getType() == 1){
                if (region != null){
                    System.out.println("Too many regions");
                    return;
                }
                region = value.getValue();
                for (String town : tmpTown){
                    context.write(NullWritable.get(), new Text(town + "," + region));
                }
                tmpTown.clear();
                continue;
            }
            context.write(NullWritable.get(), new Text(value.getValue() + "," + region));*/
        }
    }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MonProg");
    job.setNumReduceTasks(5);
    job.setJarByClass(TP7.class);
    //job.setMapperClass(MonProgMapper.class);
    job.setMapOutputKeyClass(TaggedKey.class);
    job.setMapOutputValueClass(TaggedValue.class);
    job.setReducerClass(MonProgReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setPartitionerClass(MonProgPartitioner.class);
    job.setGroupingComparatorClass(MonProgGrouper.class);
    job.setSortComparatorClass(MonProgSorter.class);
    //job.setInputFormatClass(TextInputFormat.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperTown.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperRegion.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}