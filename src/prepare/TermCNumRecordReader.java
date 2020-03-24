package prepare;

import java.io.IOException;
import java.util.HashMap;


import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class TermCNumRecordReader extends RecordReader<Text,Text>{
	private long start;
	private long end;
	private long pos;
	private FileSplit fileSplit;
	private Configuration conf;
	private int maxLineLength;
	private Text key=new Text();
	private Text value=new Text();
	private LineReader in;
	Configuration job;
	

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(in!=null){
			in.close();
		}
	}
	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("recordReader--key"+key);
		return key;
	}
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("recordReader--value"+value);
		return value;
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("getProcess");
		if(start==end){
			return 0.0f;
		}else{
			return Math.min(1.0f, (pos-start)/(float)(end-start));
		}
	}
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//System.out.println("termC record reader initialize");
		fileSplit=(FileSplit)split;
		
		job=context.getConfiguration();
		
		//bytes for a single record
		this.maxLineLength=job.getInt("mapered.linerecordreader.maxlength", Integer.MAX_VALUE); 
		
		//Split "S" is responsible for all records starting from "start" and "end" position
		start=fileSplit.getStart();
		end=start+fileSplit.getLength();
		
		//Retrieve file containing split "S"
		final Path file=fileSplit.getPath();
		FileSystem fs=file.getFileSystem(job);
		
		FSDataInputStream fileIn=fs.open(fileSplit.getPath());
		
		//if Split "S" starts at byte 0,first line will be processed
		//if Split "S" does not start at byte 0,first line has been already processed by S-1 
		//and therefore needs to be ignored
		boolean skipFirstLine=false;
		if(start != 0){
			//set the file pointer at "start-1" position
			//this to make sure we won't miss any line
			//it could happen if start is located on a EOF ??
			skipFirstLine=true;
			--start;
			fileIn.seek(start);
		}
		
		in=new LineReader(fileIn,job);
		
		//If the  line needs to be skipped,read the first line
		//and stores its content to a dummy text
		if(skipFirstLine){
			Text dummy=new Text();
			//reset "start" to "start + line offset"
			start+=in.readLine(dummy,0,(int)Math.min((long)Integer.MAX_VALUE,end-start));
		}
		//position is the actual start
		this.pos=start;
		
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Text dir=new Text(fileSplit.getPath().getParent().getName().toString());
		key.set(dir);
		int newSize=0;
		while(pos<end){
			//Read first line and store lts content to value
			newSize=in.readLine(value,maxLineLength,
                    Math.max((int) Math.min(
                            Integer.MAX_VALUE, end - pos),
                            maxLineLength));
			
			// No byte read, seems that we reached end of Split
            // Break and return false (no key / value)
			if(newSize==0){
				break;
			}
			//line is read,new position is set
			pos+=newSize;
			
			//Line is lower than Maximum record line size
			//break and return(found key value)
			if(newSize<maxLineLength){
				break;
			}
			
			//Line is too long
			//try again with position=position+line offset
			//i.e ignore line and go to next line
			//TODO: Shouldn't it be LOG.error instead ??
//			LOG.info("skiped line of size"+newSize+"at pos"+(pos-newSize));
			
	
			
		}
		
		if(newSize==0){
			key = null;
            value = null;
            return false;
		}else{
		// Tell Hadoop a new line has been found
        // key / value will be retrieved by
        // getCurrentKey getCurrentValue methods
		
			return true;
		}
		
	}
	
	
	
}
