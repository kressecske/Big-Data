package hu.elte.bigdata;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {
	private final Text wordKey = new Text("");
	private final BooleanWritable isFriends = new BooleanWritable(false);

	public void map(LongWritable ikey, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split(";");
		
		String userId = line[0];
		String[] friendIdList = line[1].split(",");
		
		for (String s : friendIdList) {
			Long uId = Long.parseLong(userId);
			Long fId = Long.parseLong(s);
			
			String key;
			if (uId < fId) {
				key = uId + "_" + fId;
			} else {
				key = fId + "_" + uId;
			}
			
			wordKey.set(key);
			isFriends.set(true);
			context.write(wordKey, isFriends);
		}
		
		for (int i=0; i<friendIdList.length-1; ++i) {
			for ( int j=i+1; j<friendIdList.length; ++j) {
				Long uId = Long.parseLong(friendIdList[i]);
				Long fId = Long.parseLong(friendIdList[j]);
				
				String key;
				if (uId < fId) {
					key = uId + "_" + fId;
				} else {
					key = fId + "_" + uId;
				}
				
				wordKey.set(key);
				isFriends.set(false);
				context.write(wordKey, isFriends);
			}
		}

	}

}
