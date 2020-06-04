import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.StdErrLog;

import java.io.IOException;
import java.util.Map;

/**
 *对物品组合列表进行计数，建立物品的同现矩阵
 */
public class Step2 {
    public static class Step2_userVectorToCooccurrenceMapper extends Mapper<Object,Text, Text, IntWritable>{
        private final static Text k=new Text();
        private final static IntWritable v=new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens=Recommend.DELIMITER.split(value.toString());
            for(int i=1;i<tokens.length;i++){
                String itemID=tokens[i].split(":")[0];
                for(int j=1;j<tokens.length;j++){
                    String itemID2=tokens[j].split(":")[0];
                    k.set(itemID+":"+itemID2);
                    context.write(k,v);
                }
            }
        }
    }

    public static class Step2_UserVectorToCooccurrenceReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result=new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable value:values)
                sum+=value.get();
            result.set(sum);
            context.write(key,result);
        }
    }

    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=Recommend.config();

        Path input=new Path(path.get("Step2Input"));
        Path output=new Path(path.get("Step2Output"));

        FileSystem fs=FileSystem.get(configuration);
//        if(fs.exists(input))
//            fs.delete(input,true);
//        fs.mkdirs(input);

        Job job= Job.getInstance(configuration,"step2");
        job.setJarByClass(Step2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Step2_userVectorToCooccurrenceMapper.class);
        job.setCombinerClass(Step2_UserVectorToCooccurrenceReducer.class);
        job.setReducerClass(Step2_UserVectorToCooccurrenceReducer.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}


















