import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.util.*;

public class FreqReducer extends Reducer<NextWord, IntWritable, NextWord, DoubleWritable>
{
	private DoubleWritable total = new DoubleWritable();
    private DoubleWritable relativefreq = new DoubleWritable();
    private Text currentWord = new Text("NOT_SET");

    @Override
    protected void reduce(NextWord key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException
	{
        if (key.getNext().equals(new Text("*")))
            if (key.getWord().equals(currentWord))
                total.set(total.get() + getTotalCount(value));
			else
			{
                currentWord.set(key.getWord());
                total.set(0);
                total.set(getTotalCount(value));
            }
		else
		{
            int count = getTotalCount(value);
            relativefreq.set((double) count / total.get());
            context.write(key, relativefreq);
        }
    }

    private int getTotalCount(Iterable<IntWritable> values)
	{
        int count = 0;
		
        for (IntWritable value : values)
            count += value.get();
		
        return count;
    }

}