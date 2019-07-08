package hadoopSecondSort;

import org.apache.hadoop.io.Text;
import org.mortbay.log.Log;

public class HadoopSecondSortParse {
	
	private String year;
	private String month;
	private String day;
	private Long airTemperature;
	private String quality;
	
	public void ncdcParse(String record){
		String airTemp;
		year = record.substring(15,19);
		month = record.substring(19,21);
		day = record.substring(21,23);
		
		if (record.charAt(87)=='+'){
			airTemp = record.substring(88,92);
		}else{
			airTemp = record.substring(87,92);
		}
		airTemperature = Long.parseLong(airTemp);
		
		quality = record.substring(92,93);		
	}
	
	public void ncdcParse(Text record){
		ncdcParse(record.toString());
	}
	
	public boolean isValidTemprature(){
		return airTemperature != 9999 
				&& quality.matches("[01459]")
				&& airTemperature <= 1000
				&& airTemperature >= -1000;
	}
	
	public String getYear(){
		return year;
	}
	
	public Long getTemprature(){
		return airTemperature;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public Long getAirTemperature() {
		return airTemperature;
	}

	public void setAirTemperature(Long airTemperature) {
		this.airTemperature = airTemperature;
	}

	public String getQuality() {
		return quality;
	}

	public void setQuality(String quality) {
		this.quality = quality;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}
}
