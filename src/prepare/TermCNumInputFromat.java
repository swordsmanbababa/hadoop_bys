package prepare;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


//TermCNumInputFromat.java
public class TermCNumInputFromat extends FileInputFormat{
	@Override
	public RecordReader createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("createRecordReader"+split.toString());
		TermCseqRecordReader reader=new TermCseqRecordReader();
		reader.initialize(split, context);
		return reader;
	}


}
