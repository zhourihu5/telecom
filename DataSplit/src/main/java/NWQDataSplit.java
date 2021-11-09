import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class NWQDataSplit {
     private static Configuration conf = new Configuration();


    public static void main(String[] args) throws Exception {

        Configuration _conf = new Configuration();

        Job job = new Job(_conf, "NWQDataSplit");
        job.setJarByClass(NWQDataSplit.class);

        //设置map输出类型
         job.setMapperClass(DataSplitMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce输出类型
        job.setReducerClass(DataSplitReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //设置输入
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job,new Path(args[0]));


        //设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




    public static class DataSplitMap extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //获取一行数据，使用\t进行分割。若分割后形成的数组大于14（角标为14的字段为日期格式数据），并且角标为14的字段不等于空。
            if (value.toString().trim().split("\\t").length > 14 && value.toString().trim().split("\\t")[14] != "") {
                //截取出数据中的日期数据（含时间格式为yyyy-MM-dd HH:mm:ss）
                String dateTime = value.toString().trim().split("\\t")[14];
                //若数据中包含空格
                if (dateTime.contains(" ")){
                    //截取出数据中的日期（格式为：yyyy-MM-dd）
                    String date = dateTime.substring(0, dateTime.indexOf(" "));
                    //输出
                    context.write(new Text(date), value);
                }
            }
        }
    }

    public static class DataSplitReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //若key内包含“-”
            if (key.toString().contains("-")){
                //在输入key值中使用“-”切分字符串
                String[] split = key.toString().split("-");
                //获取出日期数据中的年、月、日
                String year=split[0];
                String month=split[1];
                String day=split[2];

                //创建文件
                FileSystem hdfs = FileSystem.get(URI.create("hdfs://node01:8020/"), conf);
                byte[] buff = null;
                Path dfs = null;
                String datas = "";
                //遍历values,将每一条数据转字符串后使用“\r\n”进行拼接
                for (Text value : values) {
                    datas = datas + value.toString() + "\r\n";
                }

                //将字符串转换成Bytes
                buff = datas.getBytes();
                //设置HDFS目录
                dfs = new Path("/NetworkQualityInfoDatas/" +year+"/"+month+ "/"+day+ "/"+key.toString()+".txt");
                //实例输出流
                FSDataOutputStream outputStream = hdfs.create(dfs);
                //将这批数据写入HDFS
                outputStream.write(buff, 0, buff.length);
                outputStream.close();
                dfs = null;
                datas = "";
                buff = null;
                hdfs = null;
            }

        }
    }


}
