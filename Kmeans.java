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
    private List(Integer) centerValue = new ArrayList();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String new_key = tokenizer.nextToken();

        // 群中心ID
        centerID.add("K1");
        centerID.add("K2");
        centerID.add("K3");
        centerID.add("K4");
        // Random 給4中心點
        centerValue.add(20);
        centerValue.add(30);
        centerValue.add(50);
        centerValue.add(70);
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
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
    private int sum = 0;
    private int count = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        // 取得該類中心點
        context.write(key, new IntWritable(sum/count));
    }
 }
/*
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if(token == "")
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
 }

public static class SortReduce extends Reducer<IntWritable, Text, IntWritable, Text>{
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
                for(Text val : values){
                        context.write(key, val);
                }
        }
}
*/


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



