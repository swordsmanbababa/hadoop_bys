package train;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tools.Global_Env;

public class priorProbability {
	public static void main(String[] args) {
		getPriorPrb();
	}
	
	//HashMap<类名,该类的先验概率>
	public static HashMap<String,Double> getPriorPrb(){
		int count=0;
		HashMap<String,Double> hashmap=new HashMap();
		String path="hdfs://localhost:9000/user/hadoop/DocTypeNumOutput/part-r-00000";
		Configuration conf=new Configuration();
		try {
			FileSystem fs=FileSystem.get(URI.create(path),conf);
			InputStream in=null;
			in=fs.open(new Path(path));
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String line=null;
			while((line=br.readLine())!=null){
				String[] split=line.split("\\s+");
				String key=split[0];
				Double value=Math.log(Double.parseDouble(split[1])/Global_Env.TOTAL_DOC);
				hashmap.put(key, value);
				count++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hashmap;
	}
}
