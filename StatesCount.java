//**********************************************//
//******* AUTHOR: Rajveer Sidhu (rss62) ********//
//*******    CS 643-851 ; Fall 2016     ********//
//**********************************************//

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import java.lang.InterruptedException; 
import java.util.StringTokenizer;
import java.util.*;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.io.IOException;

public class StatesCount {
	static String globalfilename;
	static TreeMap <String, Integer> keyFreqMap = new TreeMap();
	static TreeMap <String, Integer>  SortedKeyMap = new TreeMap();
	static TreeMap <String, Integer> educationMap = new TreeMap();
	static TreeMap <String, Integer> politicsMap = new TreeMap();
	static TreeMap <String, Integer> sportsMap = new TreeMap();
	static TreeMap <String, Integer> agricultureMap = new TreeMap();
	static TreeMap <String, Integer> tempEachStateRank = new TreeMap();
	static TreeMap <String, Integer> sortedTempEachStateRank = new TreeMap();
	static TreeMap <String, String> statesWithWordRanks = new TreeMap();
	static String[] statesArray = {"Alabama", "Arkansas", "Arizona", "Alaska", "California","Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New_Hampshire", "New_Jersey", "New_Mexico", "New_York", "North_Carolina", "North_Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode_Island", "South_Carolina", "South_Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West_Virginia", "Wisconsin", "Wyoming" };

	//Mapper Class Begins
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	  static String filename;
	  
	  @Override
      protected void setup(Context context) throws IOException,
      InterruptedException {
		  FileSplit fileSplit = (FileSplit)context.getInputSplit();
		  filename = fileSplit.getPath().getName().toString();
		  globalfilename=filename;
		  
      }
	  private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();
    
	  /* Mapper function*/
	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String readline;
    	readline=value.toString();
    	readline=readline.replaceAll( "[^A-Za-z \n]", " " ).toLowerCase();
    	
    	StringTokenizer itr = new StringTokenizer(readline);
    	while (itr.hasMoreTokens()) {
	     	word.set(itr.nextToken());
	        context.write(word, one);
    	}
	  }
  	} //*** Mapper class ENDS
  
  
  /* Reducer Class Begins*/
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
    	  int sum = 0;
	      for(IntWritable val : values) {
	    	  sum += val.get();
	      }
	      result.set(sum);
	      
	      // Getting only required keys containing 4 keywords 
	      
	      String keyFilePath=key.toString()+"~"+globalfilename;
	      String keynamearray[]=keyFilePath.split("~");
	      String keyWithFilename=null;
	      if(keynamearray[0].equals("education")||keynamearray[0].equals("politics")||keynamearray[0].equals("sports")||keynamearray[0].equals("agriculture")){
	    	  keyWithFilename=keynamearray[0]+"~"+keynamearray[1];
	    	  keyFreqMap.put(keyWithFilename, sum);
	    	  
	    	  //*** Forming word-wise Key Maps
	    	  if(keynamearray[0].equals("education")){
	    		  educationMap.put(keyWithFilename, sum);  
	    	  }else if(keynamearray[0].equals("politics")){
	    		  politicsMap.put(keyWithFilename, sum);
	    	  }else if(keynamearray[0].equals("sports")){
	    		  sportsMap.put(keyWithFilename, sum);
	    	  }else if(keynamearray[0].equals("agriculture")){
	    		  agricultureMap.put(keyWithFilename, sum);
	    	  }
	      }
	       
    } // Reduce Function ended
    
    //**** Cleanup Function Begins
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	try{
    		
    		//*** Sorting Maps by values
    		SortedKeyMap=sortByValues(keyFreqMap);
    		 		
    		//**** PART 1 (A) Output Begins
    		
    		int count=0;
    		String outputKey;
    		context.write(new Text(" ************* OUTPUT: 1(A) - FREQUENCY OF EACH WORD IN DECREASING ORDER ***********:-"),new IntWritable(0));
  	 
    		//****1(a) Printing count of each word in decreasing order of Frequency
    		for(Map.Entry<String, Integer> topWordsEntry : SortedKeyMap.entrySet()) { 
    				outputKey=topWordsEntry.getKey();
    				context.write(new Text(topWordsEntry.getKey()),new IntWritable(topWordsEntry.getValue()));
  		  	} //1(a) Printing count of each word ends
  	  
    		//**** PART 1 (B) Output Begins
  	  	    
    		//**** Printing No of states for which each word is dominant
    		context.write(new Text("*********** OUTPUT: 1(B) - WORDWISE DOMINANT STATES COUNT ***********:-"),new IntWritable(0));
    		Integer eduCount=0;
  	  		Integer polCount=0;
  	  		Integer sportsCount=0;
  	  		Integer agricultureCount=0;
  	  		String educationKey;
  	  		try{
	  	  	for(int i=0;i<statesArray.length;i++){
	  	  			
	  	  			if(educationMap.containsKey("education~"+statesArray[i]) && politicsMap.containsKey("politics~"+statesArray[i]) && sportsMap.containsKey("sports~"+statesArray[i]) && agricultureMap.containsKey("agriculture~"+statesArray[i])){
			  	  		int educationRank=educationMap.get("education~"+statesArray[i]);
			  	  		int politicsRank=politicsMap.get("politics~"+statesArray[i]);
			  	  		int sportsRank=sportsMap.get("sports~"+statesArray[i]);
			  	  		int agricultureRank=agricultureMap.get("agriculture~"+statesArray[i]);
			  	  		  
			  	        //Counting Dominant words statewise
			  	  		if(educationRank > politicsRank && educationRank > sportsRank && educationRank > agricultureRank){
			  	  			eduCount++;
			  	  		}else if(politicsRank > educationRank && politicsRank > sportsRank && politicsRank > agricultureRank){
			      	  		polCount++;
			  	  		}else if(sportsRank > educationRank && sportsRank > politicsRank && sportsRank > agricultureRank){
			  	  			sportsCount++;
			  	  		}else if(agricultureCount > educationRank && agricultureCount > politicsRank && agricultureCount > sportsRank){
			  	  			agricultureCount++;
			  	  		}
	  	  			}
	  	  		} //for Loop ends
  	  		}catch(Exception e){
  	  			context.write(new Text("Error in Cleanup!-"+e.getMessage()),new IntWritable(0));

    		}
			  	  	
    
  	  	context.write(new Text("No. of states in which Education Dominates: "),new IntWritable(eduCount));
  	  	context.write(new Text("No. of states in which Politics Dominates: "),new IntWritable(polCount));
  	  	context.write(new Text("No. of states in which Sports Dominates: "),new IntWritable(sportsCount));
  	  	context.write(new Text("No. of states in which Agriculture Dominates: "),new IntWritable(agricultureCount));
    	
  	  	//****** PART 2: Starting Printing of States with same rank of words
  	  	context.write(new Text("*********** OUTPUT: PART 2-  STATES HAVING SAME RANKING OF ALL FOUR WORDS ;Different rank order per line ***************:-"),new IntWritable(0));
    	// Collecting states with same rank
  	  	for(int j=0;j<statesArray.length;j++){
	  	  	if(educationMap.containsKey("education~"+statesArray[j]) && politicsMap.containsKey("politics~"+statesArray[j]) && sportsMap.containsKey("sports~"+statesArray[j]) && agricultureMap.containsKey("agriculture~"+statesArray[j])){
	  	  		Integer educationRank=educationMap.get("education~"+statesArray[j]);
	  	  		//context.write(new Text("Education Rank:"+educationRank),new IntWritable(0));
	  	  		tempEachStateRank.put("Education", educationRank);
	  	  		Integer politicsRank=politicsMap.get("politics~"+statesArray[j]);
		  		//context.write(new Text("Politics Rank:"+politicsRank),new IntWritable(0));
	  	  		tempEachStateRank.put("Politics", politicsRank);
	  	  		Integer sportsRank=sportsMap.get("sports~"+statesArray[j]);
		  		//context.write(new Text("Sports Rank:"+sportsRank),new IntWritable(0));
	  	  		tempEachStateRank.put("Sports", sportsRank);
	  	  		
	  	  		Integer agricultureRank=agricultureMap.get("agriculture~"+statesArray[j]);
	  	  		tempEachStateRank.put("Agriculture", agricultureRank);
	  	  		//Sorting By value
			  	sortedTempEachStateRank=sortByValues(tempEachStateRank);
			  	String valueRanks="";
			  	//concatenating to form keys
			  	for(Map.Entry<String, Integer> topWordsEntry : sortedTempEachStateRank.entrySet()) { 
			  		valueRanks=valueRanks+">"+topWordsEntry.getKey();
			  		
			  	 } //concatenation Ends
		  		//context.write(new Text("Agriculture Rank:"+agricultureRank),new IntWritable(0));
		  		String tempState;
			  		if(statesWithWordRanks.containsKey(valueRanks)){
			  			tempState=statesWithWordRanks.get(valueRanks);
			  			tempState=tempState+","+statesArray[j];
			  			statesWithWordRanks.put(valueRanks, tempState);
			  		}else{
			  			statesWithWordRanks.put(valueRanks,statesArray[j] );
			  		}
	  	  	} // Outer if ends
  	  		tempEachStateRank.clear();
  	  		sortedTempEachStateRank.clear();
  	  	} //Outer For ends
  	  
  	  	String outputValue;
  	  	// Printing output for Part 2
  	  for(Map.Entry<String, String> topWordsEntry : statesWithWordRanks.entrySet()) { 
			outputKey=topWordsEntry.getKey();
			outputValue=topWordsEntry.getValue();
			//String tempArray[];
			if(outputValue.indexOf(',')>=0){
				outputKey=outputKey.substring(1);
				context.write(new Text(outputValue+" ; "+"Rank Order - "+outputKey),new IntWritable(0));
			}
			
  	  } 
	
    	}catch(Exception e){
    		System.out.println("Exception in Cleanup: "+ e.getMessage());
    	}
    }
    	
  }
  
  public static <K, V extends Comparable<V>> TreeMap<K, V> sortByValues(final TreeMap<K, V> map) {
	    Comparator<K> valueComparator =  new Comparator<K>() {
	        public int compare(K k1, K k2) {
	            int compare = map.get(k2).compareTo(map.get(k1));
	            if (compare == 0) return 1;
	            else return compare;
	        }
	    };
	    TreeMap<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
	    sortedByValues.putAll(map);
	    return sortedByValues;
	}
  
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "US States Word Rank");
    job.setJarByClass(StatesCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
