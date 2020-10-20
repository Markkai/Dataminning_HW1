import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyKmeans {

 public static class KM_Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private List<String> centerID = new ArrayList<>();
    private List<Integer> centerValue = new ArrayList<>();

    public void setup(Context context){
        String sCounter = context.getConfiguration().get("COUNTER"); //isFirst == 0
        int nCounter = Intrger.parseInt(sCounter);
        if(nCounter == 1){
            // flag1 = oldCentVals(/part-r-00000)
            String new_center = context.getConfiguration().get("NEW_CENTER"); 
            String line = new_center.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                String new_key = tokenizer.nextToken();  // K1 2 3 4
                String token = tokenizer.nextToken();    // values
                int new_value = Integer.parseInt(token); 
                centerID.add(new_key);
                centerValue.add(new_value);
            }
        }
        else{
            // 群中心ID
            centerID.add("K1");
            centerID.add("K2");
            centerID.add("K3");
            centerID.add("K4");
            centerValue.add((int)(Math.random() * 120 + 1));
            centerValue.add((int)(Math.random() * 120 + 1));
            centerValue.add((int)(Math.random() * 120 + 1));
            centerValue.add((int)(Math.random() * 120 + 1));
        }

    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken(); //Date
            int new_value = Integer.parseInt(token);

            List<Double> list_distance = new ArrayList<>();
            
            // 算距離
            for(int i =0; i< centerValue.size(); i++){ 
                double distance = Math.abs(new_value - centerValue.get(i));
                list_distance.add(distance);
            }
            // 找出最近中心點並分類
            for(int i= 0; i< centerValue.size(); i++){
                if(Collections.min(list_distance) == Math.abs(new_value - centerValue.get(i))){
                    
                    context.write(new Text(centerID.get(i)), new IntWritable(new_value));
                }
            }
        }
    }
 }
 public static class KM_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for(IntWritable val : values){
            sum += val.get();
            count++;
        }
        int new_value = sum/count;
        context.write(key, new IntWritable(new_value));
    }
 }



public static void main(String[] args) throws Exception {
    
    String flag1 = "NEW_CENTER";
    String flag2 = "COUNTER";
    String isFirst = "0";
    String filename = "/part-r-00000";

    //Job conf
    Configuration conf = new Configuration();
    Path inputfile = new Path(args[0]);
    Path outputfile = new Path(args[1]);

    conf.set(flag2, isFirst);                                                                                                                         
    Job job1 = new Job(conf, "Kmeans");

    Filesystem hdfs = Filesystem.get(conf);
    if(hdfs.exists(outputfile)){
        hdfs.delete(outputfile, true);
    }

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setMapperClass(KM_Map.class);
    job1.setReducerClass(KM_Reduce.class);
    job1.setJarByClass(MyKmeans.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job1, inputfile);
    FileOutputFormat.setOutputPath(job1, outputfile);
    job.waitForCompletion(true);

    isFirst = "1";
    
    int repeated = 0;
    do{
        
        Center center = new center();
        String oldCentVals = center.preCenter(new Path(outputfile + filename));
        System.out.println(oldCentVals);

        conf.set(flag1, oldCentVals);
        conf.set(flag2, isFirst);

        Job job = new Job(conf, "kmeans");

        //FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputFile)) {
            hdfs.delete(outputFile, true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(KM_Map.class);
        job.setReducerClass(KM_Reduce.class);
        job.setJarByClass(MyKmeans.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputfile);
        FileOutputFormat.setOutputPath(job, outputfile);
        job.waitForCompletion(true);

        center = new center();
        oldCentVals = center.NewCenter(outputFile);
        System.out.println(oldCentVals);

        repeated++;
        System.out.println("Generation: "+ repeated);
    }while(repeated < 30);
  }
}



