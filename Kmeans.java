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

    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{

     // Random 給4中心點 & 群中心ID
    HashMap<String, Integer> map_4k = new HashMap<String, Integer>();
    map_4k.put("K1", 20);
    map_4k.put("K2", 30);
    map_4k.put("K3", 50);
    map_4k.put("K4", 70);

    HashMap<String, List> Kx_values = new HashMap<String, Integer>();
  }

   // private List<String> centerID = new ArrayList<>();
    private List<Integer> centerValue = new ArrayList<>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            int new_value = Integer.parseInt(token);
            List<Double> list_distance = new ArrayList<>();

            Set<String> keySet = map_4k.keySet();
            // 算距離
            for(String id : keySet){ 
                double distance = Math.abs(new_value - map_4k.get(id));
                list_distance.add(distance);
            }
            // 找出最近中心點並分類
            for(String id : keySet){
                if(Collections.min(list_distance) == Math.abs(new_value - map_4k.get(id))){
                    centerValue.add(new_value);
                    Kx_values.put(id, centerValue);
                }
            }
        }
    }

    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
        Set<String> keySet2 = Kx_values.keySet();
        List<Integer> values = new ArrayList<>();

        for(String id : keySet2){
            values = Kx_values.get(id);
            for(int i= 0; i< values.size(); i++){
                context.write(new Text(id), new IntWritable(i));
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



 
