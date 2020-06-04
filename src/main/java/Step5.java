import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Step5 {
    public static class Step5_RecommendMapper extends Mapper<Object,Text,Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens=Recommend.DELIMITER.split(value.toString());
            Text k=new Text(tokens[0]);
            Text v=new Text(tokens[1]+","+tokens[2]);
            context.write(k,v);
        }
    }

    public static class Step5_RecommendReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> map=new HashMap<String, Double>();
            for(Text line:values){
                String[] tokens=Recommend.DELIMITER.split(line.toString());
                String itemID=tokens[0];
                double score=Double.parseDouble(tokens[1]);

                if(map.containsKey(itemID)){
                    map.put(itemID,map.get(itemID)+score);
                }else{
                    map.put(itemID,score);
                }
            }

            for(String mapKey:map.keySet()){
                double score=map.get(mapKey);
                Text v=new Text(mapKey+","+score);
                context.write(key,v);
            }
        }
    }

    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=Recommend.config();

        Path input = new Path(path.get("Step5Input"));
        Path output=new Path(path.get("Step5Output"));

        Job job= Job.getInstance(configuration);
        job.setJarByClass(Step5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Step5_RecommendMapper.class);
        job.setReducerClass(Step5_RecommendReducer.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);

    }
}
