import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
 */
public class Step1 {
    public static class Step1_ToItemPreMapper extends Mapper<Object,Text, Text, Text>{
        private final static Text k=new Text();
        private final static Text v=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens=Recommend.DELIMITER.split(value.toString());
            String userID=tokens[0];
            String itemID=tokens[1];
            String rating=tokens[2];
            k.set(userID);
            v.set(itemID+":"+rating);
            context.write(k,v);
        }
    }

    public static class Step1_ToUserVectorReducer extends Reducer<Text,Text,Text,Text>{
        private final static Text v=new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder=new StringBuilder();
            for(Text value:values){
                stringBuilder.append(","+value);
            }
            v.set(stringBuilder.toString().replaceFirst(",",""));
            context.write(key,v);
        }
    }

    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=Recommend.config();

        Path input=new Path(path.get("Step1Input"));
        Path output=new Path(path.get("Step1Output"));

        FileSystem fs=FileSystem.get(configuration);
        if(fs.exists(input))
            fs.delete(input,true);
        fs.mkdirs(input);
        fs.copyFromLocalFile(new Path(path.get("data")),input);

        Job job= Job.getInstance(configuration,"step1");
        job.setJarByClass(Step1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Step1_ToItemPreMapper.class);
        job.setReducerClass(Step1_ToUserVectorReducer.class);


        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}
