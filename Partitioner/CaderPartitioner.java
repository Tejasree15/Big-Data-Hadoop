package training.collabera.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CaderPartitioner extends Partitioner < Text, Text >
{
   @Override
   public int getPartition(Text key, Text value, int numReduceTasks)
   {
      String[] str = value.toString().split(",");
      int age = Integer.parseInt(str[2]);
      
      if(numReduceTasks == 0)
      {
         return 0;
      }
      
      if(age<=20)
      {
         return 0;
      }
      else if(age>20 && age<=30)
      {
         return 1 % numReduceTasks;
      }
      else
      {
         return 2 % numReduceTasks;
      }
   }
}