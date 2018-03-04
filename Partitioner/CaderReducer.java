package training.collabera.partitioner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CaderReducer extends Reducer<Text,Text,Text,IntWritable>
{
   public int max = -1;
   public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
   {
      max = -1;
			
      for (Text val : values)
      {
         String [] str = val.toString().split(",");
         int salary = Integer.parseInt(str[4]);
         if(salary > max)
         max = salary;
      }
			
      context.write(new Text(key), new IntWritable(max));
   }
}
