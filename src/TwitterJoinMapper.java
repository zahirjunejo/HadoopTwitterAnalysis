import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

	private HashMap<String, String> athleteNames;

	// private TextIntPair pair = new TextIntPair();
	// private LongWritable volume = new LongWritable();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    String entry = value.toString();
    String[] myData = entry.split(";");

		if(myData.length == 4 && myData[0].matches("[0-9]+")){

			String tweet = myData[2];
			int tweetSize = myData[2].length();
			if(tweetSize < 140){
						for(String name : athleteNames.keySet()){
							if(tweet.contains(name)){
								IntWritable one = new IntWritable(1);

								// Uncomment this to run part c1
								// Text nameKey = new Text(name);
								// context.write(nameKey, one);

								// Uncomment this to run part c2
								Text sportValue = new Text(athleteNames.get(name));
								context.write(sportValue, one);
							}
						}
			}

		}

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

    athleteNames = new HashMap<String, String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {

			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				String[] fields = line.split(",");
				if(fields.length == 11){
					// Athlete name and the sport
					athleteNames.put(fields[1], fields[7]);
				}
			}
			br.close();


		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
