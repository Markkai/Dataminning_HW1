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

public class Kmeans {

 public static class KM_Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private List<String> centerID = new ArrayList<>();
    private List<Integer> centerValue = new ArrayList<>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
 

        // 群ID
        centerID.add("K1");
        centerID.add("K2");
        centerID.add("K3");
        centerID.add("K4");
        // 隨機4中心
        centerValue.add(20);
        centerValue.add(30);
        centerValue.add(50);
        centerValue.add(70);

        while(tokenizer.hasMoreTokens()){
            String column1 = tokenizer.nextToken(); // Date
            String column2 = tokenizer.nextToken(); // Dali
            String column3 = tokenizer.nextToken(); //PM2.5

            int new_value = Integer.parseInt(column3);
            List<Double> list_distance = new ArrayList<>();

            // 第一個值和 K1,2,3,4 算距離找最小
            for(int i = 0;i< centerValue.size();i++){ 
                double distance = Math.abs(new_value - centerValue.get(i));
                list_distance.add(distance);
            }
            // 找出最近中心點並分類
            for(int i= 0;i< centerValue.size();i++){
                if(Collections.min(list_distance) == Math.abs(new_value - centerValue.get(id))){
                    context.write(new Text(centerID.get(i)), new IntWritable(new_value));
                }
            }
        }
    }
 }

 public static class KM_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int sum = 0;
    private int count = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        for(IntWritable val : values){
            sum += val.get();
            count++;
        }
        context.write(key, new IntWritable(sum/count));
    }
 }
 
}

public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();                                                                                                                                
    Job job = new Job(conf, "Kmeans");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(KM_Map.class);
    job.setReducerClass(KM_Reduce.class);
    job.setJarByClass(Kmeans.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);


    /*
    Configuration conf2 = new Configuration();

    Job job2 = new Job(conf2, "Sort_count");
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    job.setMapperClass(SortMap.class);
    job.setReducerClass(SortReduce.class);
    job.setJarByClass(WordCount.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job2.waitForCompletion(true);
*/
    }
}



 
