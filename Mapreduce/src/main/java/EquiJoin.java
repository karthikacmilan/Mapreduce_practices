package EquiJoin.EquiJoin;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {

/*
* Map
*/
//store in string buffer 
StringBuffer bufferst =new StringBuffer();
StringTokenizer it=new StringTokenizer(value.toString(), ",");
String lines = value.toString();
StringTokenizer tokenizer = new StringTokenizer(lines);
String tuplename=it.nextToken();

int secondkey=Integer.parseInt(it.nextToken());

while(it.hasMoreTokens())
{
bufferst.append(it.nextToken()+",");
}

String appendvalues=bufferst.toString();
//set of tuples as table name / column / values
//key and values
context.write(new IntWritable(secondkey), new Text(tuplename+","+secondkey+","+appendvalues));

}
}


class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

@Override
public void reduce(IntWritable key, Iterable<Text> values, Context text)
throws IOException, InterruptedException {

/*
* Reduce
*/

ArrayList<String> Rvalues=new ArrayList<String>();
ArrayList<String> Svalues=new ArrayList<String>();
String inittablename = "";
String line="";
int checker = 0;
for(Text value:values){
    line = value.toString();
    if(checker == 0){Rvalues.add(line);
        checker = 1;
        inittablename = line.substring(0, line.indexOf(','));
    }
    else{
        String newtable = line.substring(0, line.indexOf(','));
        if(newtable.equals(inittablename))
            Rvalues.add(line);
        else
            Svalues.add(line);
    }
}
for(int i=0;i<Rvalues.size();i++)
for(int j=0;j<Svalues.size();j++)
text.write(NullWritable.get(), new Text(Rvalues.get(i)+","+Svalues.get(j)));


}
}
public class EquiJoin {
  /**
    * @param args
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
    */
public static void main(String[] args) throws Exception {

Job job = Job.getInstance(new Configuration());
job.setOutputValueClass(Text.class);
job.setOutputKeyClass(NullWritable.class);
job.setMapOutputKeyClass(IntWritable.class);

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
Path outputPath = new Path(args[1]);
job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.setInputPaths(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
//outputPath.getFileSystem(conf).delete(outputPath);

job.setJarByClass(EquiJoin.class);

job.submit();


}
}