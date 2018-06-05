import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.*;

public class FreqMapper extends Mapper<LongWritable, Text, NextWord, IntWritable>
{
	private NextWord twoWords = new NextWord();
	 @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		int neighbors = context.getConfiguration().getInt("neighbors", 1);
        String[] tokens = value.toString().split("\\s+");

        if (tokens.length > 1)
            for (int i = 0; i < tokens.length; i++)
                if(tokens[i].matches("^[A-Za-z]+$"))
				{
                    tokens[i] = tokens[i].trim();

                    if(tokens[i].equals(""))
                        continue;
                    twoWords.setWord(tokens[i].toLowerCase());

                    int start = 0;
                    if(i > neighbors )
                       start = i - neighbors;

                   	int end = i + neighbors;
                    if(i + neighbors >= tokens.length)
                        end = tokens.length - 1;

                    for (int j = start; j <= end; j++)
					{
                        if (j == i)
							continue;
						
                        if(tokens[j].matches("^[A-Za-z]+$"))
						{
							tokens[j] = tokens[j].trim();
                          	twoWords.setNext(tokens[j].toLowerCase());
                          	context.write(twoWords, new IntWritable(1));
                        }
                     }

                    twoWords.setNext("*");
                    context.write(twoWords, new IntWritable(end-start));
                }
	}

}