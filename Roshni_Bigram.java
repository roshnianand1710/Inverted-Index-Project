
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class WordCount {
   
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        
        private Text Bigrams = new Text();
        
        private Text documentID = new Text();
        //mapper function
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] document = value.toString().split("\t", 2);
            String text = document[1].toLowerCase();
            
            text = text.replaceAll("[^a-z\\s]", " ");
            text = text.replaceAll("\\s+", " ");

            documentID.set(document[0]);
            
            StringTokenizer token = new StringTokenizer(text);
            String prev = token.nextToken();

            while (token.hasMoreTokens()) {
                
                String curr = token.nextToken();
                String currentBigram = prev + " " + curr;
                
                if (isDesiredBigram(currentBigram)) {
                    
                    Bigrams.set(currentBigram);
                    context.write(Bigrams, documentID);
                }
                prev = curr;
            }
        }


        private boolean isDesiredBigram(String bigram) {
            //Checking if the bigram is one of the desired bigrams
            return bigram.equals("computer science") ||
                    bigram.equals("information retrieval") ||
                    bigram.equals("power politics") ||
                    bigram.equals("los angeles") ||
                    bigram.equals("bruce willis");
            
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //reducer function
            HashMap<String, Integer> myHashMap = new HashMap<>();
            for (Text val : values) {
                
                String documentID = val.toString();
                myHashMap.put(documentID, myHashMap.getOrDefault(documentID, 0) + 1);
                
            }

            StringBuilder stringBuilder = new StringBuilder();
            
            for (String k : myHashMap.keySet())
                
                stringBuilder.append(k).append(":").append(myHashMap.get(k)).append("\t");

            result.set(stringBuilder.substring(0, stringBuilder.length() - 1));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //main function
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Roshni's Bigrams");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setReducerClass(WordCount.IndexReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
