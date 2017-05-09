package hu.elte.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, BooleanWritable, Text, Text> {
	private final Text wordNum = new Text("");

	public void reduce(Text _key, Iterable<BooleanWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		boolean offer = true;
		for (BooleanWritable val : values) {
			if (val.get())  {
				offer = false;
			}
		}
		
		if(offer) {
			wordNum.set("");
			context.write(_key, wordNum);
		}
	}
}
