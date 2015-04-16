import java.io.BufferedReader;
import java.io.BufferedWriter; 
import java.io.EOFException; 
import java.io.File; 
import java.io.FileNotFoundException; 
import java.io.FileReader;
import java.io.FileWriter; 
import java.io.IOException;
import java.lang.Comparable; 
import java.lang.Runtime; 
import java.lang.StringBuilder; 
import java.lang.Thread; 
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount 
{
  final static int NUM_CHAPTERS = 61; 
  static Job job; 
  final static String resultFileName = "output.txt"; 

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>
    {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
      StringTokenizer itr = new StringTokenizer(value.toString());

      // getting the chapter name we are mapping
      String filename = ((FileSplit) context.getInputSplit()).getPath().toString(); 
      String chapterString = filename.substring(filename.indexOf("chap"));  
   
      while (itr.hasMoreTokens()) 
      {

        String fileWord = itr.nextToken().toLowerCase();
        StringBuilder sb = new StringBuilder(fileWord);  
        
        // replace non characters with spaces
        for (int i = 0; i < fileWord.length(); i += 1)
        {
          if ((fileWord.charAt(i) < 'a') 
              || (fileWord.charAt(i) > 'z'))
            sb.setCharAt(i, ' '); 
        }

        // remove spaces from above replacement and send remaining strings
        String[] stringsToSend = sb.toString().split("\\s+");
        for (int i = 0; i < stringsToSend.length; i += 1)
        {
          if (stringsToSend[i].isEmpty() == false) 
          {
            word.set(stringsToSend[i] + "." + chapterString);
            context.write(word, one);
          }
        }
      }
    }
  }

  private int getChapter(String ch) 
  {
    return Integer.parseInt(ch.substring(ch.indexOf("p") + 1)); 
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException 
    {
      int sum = 0;

      for (IntWritable val : values) 
        sum += val.get();

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Runtime r = Runtime.getRuntime(); 

    Configuration conf = new Configuration();

    job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // pass all chapters to input 
    String chPre = "/input/chap";
    for (int i = 0; i <= NUM_CHAPTERS; i += 1)
    {
      String filename = chPre; 
      if (i < 10)
        filename += "0" + i; 
      else
        filename += i; 
      FileInputFormat.addInputPath(job, new Path(filename)); 
    }
    FileOutputFormat.setOutputPath(job, new Path("/output"));    
    job.waitForCompletion(true); 
   
    r.exec("hdfs dfs -copyToLocal /output"); 
    
    try{
      Thread.sleep(3000); 
    } catch (InterruptedException e){}

    BufferedReader br = new BufferedReader(new FileReader("output/part-r-00000"));
    
    ArrayList<A5Out> aList = new ArrayList<A5Out>(); 

    String line = br.readLine(); 
    while (line != null)
    {
      aList.add(new A5Out(line)); 
      line = br.readLine(); 
    }

    Map<String, A5Word> count = new TreeMap<String, A5Word>();
    
    for (A5Out mrRes : aList) {
      
      // get fields to print
      String word = mrRes.word;
      String chapter = mrRes.chapter;
      Integer numOccurances = mrRes.numOccurances;
      
      // map word to A5Word in hashmap, and map A5Word from hashmap to treemap
      if (count.get(word) == null) {
        count.put(word, new A5Word(word));
        count.get(word).count.put(numOccurances + "." + chapter,
            chapter);
      } else {
        count.get(word).count.put(numOccurances + "." + chapter,
            chapter);
      }
    }
    
    java.util.Iterator<Entry<String, A5Word>> entries = count.entrySet()
        .iterator();
    ArrayList<A5Word> work = new ArrayList<A5Word>();
    BufferedWriter bw = new BufferedWriter(
      new FileWriter((
        new File(resultFileName)).getAbsoluteFile())
    );
    while (entries.hasNext()) {
      Map.Entry entry = (Map.Entry) entries.next();
      String key = (String) entry.getKey();
      A5Word value = (A5Word) entry.getValue();
      work.add(value);
      value.print();
      // try {
      //   value.printToFile(resultFileName, bw); 
      // } catch (IOException e) {}
    }
    bw.close(); 
  }

  private static class A5Out implements Comparable<A5Out> {
    public String word;
    public String chapter;
    public Integer numOccurances;

    public A5Out(String line) {
      String[] comp = line.split("\\s+");
      if (comp.length == 2) {
        String[] wordAndChap = comp[0].split("\\.");
        this.word = wordAndChap[0];
        this.chapter = wordAndChap[1];
        this.numOccurances = (Integer) Integer.parseInt(comp[1]);
      }
    }

    public int compareTo(A5Out that) {
      int result;
      result = this.numOccurances.compareTo(that.numOccurances);

      if (result == 0) {
        result = this.chapter.compareTo(that.chapter);
        if (result == 0)
          result = this.word.compareTo(that.word);
      }
      return result;
    }

    public String toString() {
      String retStr;
      retStr = word + "\n<" + chapter + ", " + numOccurances + ">\n";
      return retStr;
    }
  } // A5Out

  private static class A5Word {
    public String word;
    public HashMap<String, String> count = new HashMap<String, String>();

    public A5Word(String inWord) {

      this.word = inWord;
      this.count = new HashMap<String, String>();
    }

    public void print() {
      System.out.println(word);
      java.util.Iterator<Entry<String, String>> entries = count
          .entrySet().iterator();

      A5OList aout = new A5OList(); 
      while (entries.hasNext()) {
        Map.Entry entry = (Map.Entry) entries.next();
        String key = (String) entry.getKey();
        String value = (String) entry.getValue();
        String retStr;
        aout.add(value, Integer.parseInt(key.substring(0, key.indexOf(".")))); 
      }
      aout.sort(); 
      System.out.println(aout.toString());
    }

    public void printToFile(String filename, BufferedWriter bw) 
      throws IOException {
      
      bw.write(word);
      bw.newLine(); 
      java.util.Iterator<Entry<String, String>> entries = count
          .entrySet().iterator();
      A5OList aout = new A5OList(); 
      while (entries.hasNext()) {
        Map.Entry entry = (Map.Entry) entries.next();
        String key = (String) entry.getKey();
        String value = (String) entry.getValue();
        String retStr;
        aout.add(value, Integer.parseInt(key.substring(0, key.indexOf(".")))); 
      }
      aout.sort();
      bw.write(aout.toString());
      bw.newLine(); 
    }
  } // A5Word
    

  private static class A5OList 
  {
    ArrayList<a5obj> list;      

    public A5OList(){
      list = new ArrayList<a5obj>();
    }

    public void add(String str, int numOccurances){
      list.add(new a5obj(str, numOccurances)); 
    }

    public void sort(){
      Collections.sort(list); 
    }

    public String toString(){
      String result = ""; 
      for (a5obj out: this.list)
        result += (out.toString() + "\n"); 
      return result; 
    }

    private class a5obj implements Comparable<a5obj> {
      String  str; 
      Integer occ; 
      
      public a5obj(String s, int occ) { 
        str = s; this.occ = (Integer) occ; 
      }
      
      public int compareTo(a5obj that) {
        int result = this.occ.compareTo(that.occ); 
        result = 0 - result; 
        if (result == 0)
          result = this.str.compareTo(that.str);
        return result; 
      }

      public String toString(){
        return ("<" + str + ", " + occ.toString()); 
      }
    } // a5obj

  } // A5OList
}

