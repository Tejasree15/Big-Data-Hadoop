package tact.trainings.mapreduce.wordcount;


import java.io.IOException;


import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
 {


	@Override

	protected void reduce(Text key, Iterable<IntWritable> values, Contextcontext)throwsIOException,InterruptedException
        {


		int count = 0;

		for (IntWritable current : values)
                {

			count += current.get();

		}

		context.write(key, new IntWritable(count));

	}


}