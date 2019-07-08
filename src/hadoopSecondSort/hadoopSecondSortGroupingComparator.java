package hadoopSecondSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class hadoopSecondSortGroupingComparator extends WritableComparator {
	
	public hadoopSecondSortGroupingComparator(){
		super(HadoopSecondSortKey.class,true);
	}
	
	public int compare(WritableComparable wc1,WritableComparable wc2){
		HadoopSecondSortKey hsk1 = (HadoopSecondSortKey)wc1;
		HadoopSecondSortKey hsk2 = (HadoopSecondSortKey)wc2;
		return hsk1.getYearMonth().compareTo(hsk2.getYearMonth());		
	}
}
