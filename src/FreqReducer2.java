import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.util.*;

public class FreqReducer2 extends Reducer<DoubleWritable, NextWord, NextWord, DoubleWritable>
{
	private int i = 0;
	
    @Override
    protected void reduce(DoubleWritable key, Iterable<NextWord> values, Context context) throws IOException, InterruptedException
	{
		for (NextWord value : values)
		{
			if(i >= 100)
				break;
			
			if(key.get() == 1.0)
				continue;
			
			context.write(value, key);
			i++;
        }
	}
}