package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Etap1b {

    public static class AlcoholCategoriesMapper
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            if (value.toString().startsWith("Inv")) {
                return; //TODO Ugly, better exit strategy
            }

            String[] data = Utils.parseCSVLine(value.toString());

            Text alcoholCategoryResult = alcoholCategoryResult(data[11]);

            context.write(new Text(data[0]), alcoholCategoryResult);
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if (args.length != 3) {
                System.err.println("Incorrect number of arguments");
                System.exit(2);
            }
            Job job = new Job(conf, "Etap1b Job");
            job.setJarByClass(Etap1b.class);
            job.setMapperClass(Etap1b.AlcoholCategoriesMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));


            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Text alcoholCategoryResult(String alcoholCategory) {
        if (alcoholCategory.contains("WHISKIES")) {
            return new Text("Whiskies");
        } else if (alcoholCategory.contains("VODKA")) {
            return new Text("Vodka");
        } else if (alcoholCategory.contains("IMPORTED")) {
            return new Text("Imported");
        } else if (alcoholCategory.contains("PROOF")) {
            return new Text("Proof");
        } else if (alcoholCategory.contains("RUM")) {
            return new Text("Rum");
        } else if (alcoholCategory.contains("AMERICAN")) {
            return new Text("American");
        } else if (alcoholCategory.contains("CANADIAN")) {
            return new Text("Canadian");
        } else if (alcoholCategory.contains("FLAVORED")) {
            return new Text("Flavored");
        } else if (alcoholCategory.contains("LIQUEURS")) {
            return new Text("Liqueurs");
        } else if (alcoholCategory.contains("STRAIGHT")) {
            return new Text("Straight");
        } else if (alcoholCategory.contains("SCHNAPPS")) {
            return new Text("Schnapps");
        } else if (alcoholCategory.contains("BOURBON")) {
            return new Text("Bourbon");
        } else if (alcoholCategory.contains("BRANDIES")) {
            return new Text("Brandies");
        } else if (alcoholCategory.contains("SPICED")) {
            return new Text("Spiced");
        } else if (alcoholCategory.contains("BLENDED")) {
            return new Text("Blended");
        } else if (alcoholCategory.contains("TEQUILA")) {
            return new Text("Tequila");
        } else if (alcoholCategory.contains("GRAPE")) {
            return new Text("Grape");
        } else if (alcoholCategory.contains("ISLANDS")) {
            return new Text("Islands");
        } else if (alcoholCategory.contains("PUERTO")) {
            return new Text("Puerto");
        } else if (alcoholCategory.contains("RICO")) {
            return new Text("Rico");
        } else {
            return new Text("Other");
        }
    }

}

