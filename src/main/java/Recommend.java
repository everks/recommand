import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommend {
    public static final String HDFS="hdfs://127.0.0.1:9000";
    public static final Pattern DELIMITER=Pattern.compile("[\t,]");

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        if(args.length<1){
            System.err.println("Usage: Recommend <input>");
            System.exit(1);
        }

        Map<String,String> path=new HashMap<String,String>();
        path.put("data",args[0]);

        path.put("Step1Input", HDFS + "/user/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");

        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");


        path.put("Step3Input", path.get("Step1Output"));
        path.put("Step3Output", path.get("Step1Input") + "/step3");

        path.put("Step4Input1", path.get("Step3Output"));
        path.put("Step4Input2", path.get("Step2Output"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        path.put("Step5Input",path.get("Step4Output"));
        path.put("Step5Output",path.get("Step1Input")+"/step5");

        Step1.run(path);
        Step2.run(path);
        Step3.run(path);
        Step4.run(path);
        Step5.run(path);
        System.exit(0);
    }

    public static Configuration config(){
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS",HDFS);
        return configuration;
    }
}
