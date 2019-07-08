package hadoopSecondSort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class HadoopSecondSortMap 
			extends Mapper<LongWritable, Text, HadoopSecondSortKey, HadoopSecondSortKey> {
	
	private HadoopSecondSortParse parse = new HadoopSecondSortParse();
	private final HadoopSecondSortKey sortKey = new HadoopSecondSortKey();
	private final HadoopSecondSortKey sortKey1 = new HadoopSecondSortKey();
	//private final HadoopSecondSortValue sortValue = new HadoopSecondSortValue();
	
	@Override
	public void map(LongWritable key, Text values, 
			Mapper<LongWritable, Text, HadoopSecondSortKey, HadoopSecondSortKey>.Context context) throws IOException, InterruptedException{
		//解析类进行初始化
		parse.ncdcParse(values);		
		if (parse.isValidTemprature()){	
			String yearMonth = parse.getYear()+parse.getMonth();
			String day = parse.getDay();
			sortKey.set(yearMonth,day);	
			String keyDay = day;
			Long temperature = parse.getTemprature();
			sortKey1.set(keyDay,temperature.toString());	
			context.write(sortKey,sortKey1);
		}
	}


	
}
