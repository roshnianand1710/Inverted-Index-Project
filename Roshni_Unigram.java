import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;
import java.io.IOException;
import java.util.StringTokenizer;

public class RoshniInvertedIndex {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text documentID = new Text();
        //mapper function 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] document = value.toString().split("\t", 2);
            String text = document[1].toLowerCase();
            
            text = text.replaceAll("[^a-z\\s]", " ");
            text = text.replaceAll("\\s+", " ");

            documentID.set(document[0]);
            //tokenizing the text and writing the word and documentID to the context
            StringTokenizer token = new StringTokenizer(text);
            
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, documentID);
            }
        }
    }
    //reducer function
    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //creating a hashmap to store the documentID and its frequency
            HashMap<String, Integer> myHashMap = new HashMap<>();
            for (Text val : values) {
                String documentID = val.toString();
                myHashMap.put(documentID, myHashMap.getOrDefault(documentID, 0) + 1);
            }

            StringBuilder str = new StringBuilder();
            
            for (String k : myHashMap.keySet())
                str.append(k).append(":").append(myHashMap.get(k)).append("\t");

            result.set(str.substring(0, str.length() - 1));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Roshni's Inverted Index");
        
        job.setJarByClass(RoshniInvertedIndex.class);
        //setting the mapper and reducer class
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IndexReducer.class);
        //setting the output key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
