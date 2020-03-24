package train;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tools.Global_Env;


//ConditionalProbility.java
public class ConditionalProbility {
	public static void main(String[] args) {
		getT_C();
	}
	
	//get the prior Conditional Probility of docC--term
	//HaspMap<<类名，单词>，概率>
	public static HashMap<HashMap<String,String>,Double>  getT_C(){
		int TOATAL_TERM=0;
		HashMap<HashMap<String,String>,Double> hashMap=new HashMap();
		HashMap<String,Integer> total_count=new HashMap(); 
		String path="hdfs://localhost:9000/user/hadoop/TermCNumOutput/part-r-00000";
		Configuration conf=new Configuration();
		try {
			FileSystem fs=FileSystem.get(URI.create(path),conf);
			InputStream in=null;
			in=fs.open(new Path(path));
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String line=null;
			String curDocType=null;
			//totalWordOfC record the word number of the current classC
			int totalWordOfC=0;
			while((line=br.readLine())!=null){
				totalWordOfC++;
				String[] split=line.split("\\s+");
				String docType=split[0];
				String term=split[1];
				Double value=Double.parseDouble((split[2]))+1;
				HashMap<String,String> key=new HashMap();
				key.put(docType, term);
				hashMap.put(key,value);
				
				if(curDocType==null){
					curDocType=docType;
				}
				if(!curDocType.equals(docType)){
					System.out.println("curDocType:"+curDocType+" doctype  "+docType+" "+totalWordOfC);
					total_count.put(curDocType, totalWordOfC+Global_Env.TOATAL_TERM);
					TOATAL_TERM+=totalWordOfC;
					curDocType=docType;
					totalWordOfC=0;
				}
			}
			
			total_count.put(curDocType, totalWordOfC+Global_Env.TOATAL_TERM);
			
			 for(Map.Entry<HashMap<String, String>, Double> entry: hashMap.entrySet())
		    {
				 Double value=entry.getValue();
				 HashMap<String,String> hashKey=entry.getKey();
				 int dev=0;
				 for(String key:hashKey.keySet())
				    {
				     dev=total_count.get(key);
				    }
				 entry.setValue(value/dev);
			   //  System.out.println("Key: "+ entry.getKey()+ " Value: "+entry.getValue());
		    }
			System.out.println("  TOATAL_TERM"+TOATAL_TERM);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hashMap;
	}
}
