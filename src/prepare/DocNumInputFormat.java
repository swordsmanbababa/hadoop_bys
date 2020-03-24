package prepare;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class   DocNumInputFormat  extends FileInputFormat<Text,IntWritable>{

	@Override
	public RecordReader<Text, IntWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		DocNumRecordReader reader=new DocNumRecordReader();
		//System.out.println("createRecordReader"+split.toString());
		reader.initialize(split, context);
		return reader;
	}



	
}
