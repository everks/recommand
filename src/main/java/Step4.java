import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Step4 {

    public static class Step4_PartialMultiplyMapper extends Mapper<Object,Text,Text, Text>{
        private String flag;    // A 同现矩阵 or B 评分矩阵

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split= (FileSplit) context.getInputSplit();
            flag=split.getPath().getParent().getName();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens=Recommend.DELIMITER.split(value.toString());

            if(flag.equals("step2")){ //同现矩阵
                String[] v1=tokens[0].split(":");
                String itemID1=v1[0];
                String itemID2=v1[1];
                String num=tokens[1];

                Text k=new Text(itemID1);
                Text v=new Text("A:"+itemID2+","+num);
                context.write(k,v);
            }else if(flag.equals("step3")){ //评分矩阵
                String[] v2=tokens[1].split(":");
                String itemID=tokens[0];
                String userID=v2[0];
                String rating=v2[1];

                Text k=new Text(itemID);
                Text v=new Text("B:"+userID+","+rating);
                context.write(k,v);
            }
        }
    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,String> mapA=new HashMap<String, String>();
            Map<String,String> mapB=new HashMap<String, String>();

            for(Text line:values){
                String val=line.toString();

                if(val.startsWith("A:")){
                    String[] kv=Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0],kv[1]);
                }else if(val.startsWith("B:")){
                    String[] kv=Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0],kv[1]);
                }
            }

            double result=0;
            for(String keyA:mapA.keySet()){
                int num=Integer.parseInt(mapA.get(keyA));
                for(String keyB:mapB.keySet()){
                    double rating=Double.parseDouble(mapB.get(keyB));
                    result=num*rating;

                    Text k=new Text(keyB);
                    Text v=new Text(keyA+","+result);
                    context.write(k,v);
                }
            }
        }
    }

    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=Recommend.config();

        Path input1=new Path(path.get("Step4Input1"));
        Path input2=new Path(path.get("Step4Input2"));
        Path output=new Path(path.get("Step4Output"));

        Job job=Job.getInstance(configuration);
        job.setJarByClass(Step4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_AggregateReducer.class);

        FileInputFormat.addInputPath(job,input1);
        FileInputFormat.addInputPath(job,input2);
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}
