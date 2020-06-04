import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 合并同现矩阵和评分矩阵
 */
public class Step3 {
    public static class Step31_UserVectorSplitterMapper extends Mapper<Object,Text, Text, Text>{
        private final static Text k=new Text();
        private final static Text v=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            for (int i = 1; i < tokens.length; i++) {
                String[] vector = tokens[i].split(":");
                String itemID = vector[0];
                String rating = vector[1];

                k.set(itemID);
                v.set(tokens[0] + ":" + rating);
                context.write(k, v);
            }
        }
    }

    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=Recommend.config();

        Path input=new Path(path.get("Step3Input"));
        Path output=new Path(path.get("Step3Output"));

        Job job=Job.getInstance(configuration,"step3");
        job.setJarByClass(Step3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Step31_UserVectorSplitterMapper.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);

    }
}
