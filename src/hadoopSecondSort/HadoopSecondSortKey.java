package hadoopSecondSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HadoopSecondSortKey implements WritableComparable<HadoopSecondSortKey> {

	private String yearMonth;
	private String day;
	
	public HadoopSecondSortKey(String yearMonth,String day){
		this.yearMonth = yearMonth;
		this.day = day;
	}
	
	public HadoopSecondSortKey(){}
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.yearMonth = in.readUTF();
		this.day = in.readUTF();		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.yearMonth);
		out.writeUTF(this.day);		
	}


	@Override
	public int compareTo(HadoopSecondSortKey SortKey) {
		if(this.yearMonth.compareTo(SortKey.yearMonth) != 0){
			return this.yearMonth.compareTo(SortKey.yearMonth);
		}		
		else if((Integer.parseInt(this.day))==(Integer.parseInt(SortKey.day))){
			return 0;
		}else if((Integer.parseInt(this.day))>(Integer.parseInt(SortKey.day))){
			return 1;
		}else{
			return -1;
		}
	}

	public String getYearMonth() {
		return yearMonth;
	}

	public void setYearMonth(String yearMonth) {
		this.yearMonth = yearMonth;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}
	
	public void set(String yearMonth,String day){
		this.yearMonth = yearMonth;
		this.day = day;
	}
	
	
	public static class HadoopSecondSortKeyComparator extends WritableComparator {
		public HadoopSecondSortKeyComparator() {
			super(HadoopSecondSortKey.class,true);
		}

		public int compare(WritableComparable wc1,WritableComparable wc2) {
			HadoopSecondSortKey hsk1 = (HadoopSecondSortKey)wc1;
			HadoopSecondSortKey hsk2 = (HadoopSecondSortKey)wc2;
			if(hsk1.getYearMonth().compareTo(hsk2.getYearMonth()) == 0){
				int day1 = Integer.parseInt(hsk1.getDay());
				int day2 = Integer.parseInt(hsk2.getDay());
				if(day1 == day2){
					return 0;
				}else if (day1 > day2){
					return 1;
				}else{
					return -1;
				}
				
			}else{
				return hsk1.getYearMonth().compareTo(hsk2.getYearMonth());
			}
		}
	}

	static { // register this comparator
		WritableComparator.define(HadoopSecondSortKey.class,
				new HadoopSecondSortKeyComparator());
	}


}
