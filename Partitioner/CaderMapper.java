package training.collabera.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CaderMapper extends Mapper<LongWritable,Text,Text,Text>
{
   public void map(LongWritable key, Text value, Context context)
   {
      try{
         String[] str = value.toString().split(",");
         String gender=str[3];
         context.write(new Text(gender), new Text(value));
      }
      catch(Exception e)
      {
         System.out.println(e.getMessage());
      }
   }
}