import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
  
  public class FinalKmeansReducer
       extends Reducer<IntWritable,Text,NullWritable,Text> {
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        for(Text value: values){
            context.write(NullWritable.get(), new Text(key.get()+","+value.toString()));
        }
    }
  }
