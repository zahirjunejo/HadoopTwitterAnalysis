import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.LinkedList;
import java.util.regex.*;
import java.util.Date;
import java.time.ZoneOffset;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // BigDataCourseWorkPartA(key, value, context);
      // BigDataCourseWorkPartB(key, value, context);
       BigDataCourseWorkPartB2(key, value, context);
    }

    public void BigDataCourseWorkPartA(Object key, Text value, Context context) throws IOException, InterruptedException{
      String entry = value.toString();
      String[] myData = entry.split(";");

      if(myData.length == 4 && myData[0].matches("[0-9]+")){
        int tweetSize = myData[2].length();
        if(tweetSize < 140){
          String bin = "";
          for(int lower_bound = 1, upper_bound = 5 ; bin == "";){
            if(tweetSize >= lower_bound && tweetSize <= upper_bound){
              bin = Integer.toString(upper_bound);
              data.set(bin);
              context.write(data, one);
            }

            lower_bound+=5;
            upper_bound+=5;
          }
        }

      }
    }

    public void BigDataCourseWorkPartB(Object key, Text value, Context context) throws IOException, InterruptedException{
      String entry = value.toString();
      String[] myData = entry.split(";");

      if(myData.length == 4 && myData[0].matches("[0-9]+")){
		int tweetSize = myData[2].length();
		if(tweetSize < 140){
		  Long epochTime = Long.parseLong(myData[0]);
          LocalDateTime date = LocalDateTime.ofEpochSecond(epochTime / 1000, 0, ZoneOffset.of("-02:00"));
          data.set(Integer.toString(date.getHour() + 1));
          context.write(data, one);	
		}

      }

    }

    public void BigDataCourseWorkPartB2(Object key, Text value, Context context) throws IOException, InterruptedException{
      String entry = value.toString();
      String[] myData = entry.split(";");
      int busiestHour = 23;

      if(myData.length == 4 && myData[0].matches("[0-9]+")){
		int tweetSize = myData[2].length();
		if(tweetSize < 140){
			Long epochTime = Long.parseLong(myData[0]);
			LocalDateTime date = LocalDateTime.ofEpochSecond(epochTime / 1000, 0, ZoneOffset.of("-02:00"));
			if(date.getHour() == busiestHour){
			  String tweet = myData[2];
			  String hashRegex = "(#\\w+)";
			  Pattern pat = Pattern.compile(hashRegex);
			  Matcher match = pat.matcher(tweet);
			  while(match.find()){
				String hashtag = match.group(1);
				data.set(hashtag);
				context.write(data, one);
			  }
			}
		}
		  


      }

    }


}
