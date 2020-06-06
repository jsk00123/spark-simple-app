package com.example.chapter4

import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.{SparkConf, SparkContext}

object SundayCount {

	def main(args: Array[String]) {
		if(args.length < 1) {
			throw new IllegalArgumentException("명령 인수에 날자가 기록된 파일의 경로를 지정해 주세요.")
		}

	val filePath = args(0)
	val conf = new SparkConf().setAppName("spark-simple-app").setMaster("local")
	val sc = new SparkContext(conf)

	try {
		val textRDD = sc.textFile(filePath)
		
		val dateTimeRDD = textRDD.map { dateStr =>
			val pattern = 
				DateTimeFormat.forPattern("yyyyMMdd")
			DateTime.parse(dateStr, pattern)
		}

	val sundayRDD = dateTimeRDD.filter { dateTime =>
		dateTime.getDayOfWeek == DateTimeConstants.SUNDAY
	}

	val numOfSunday = sundayRDD.count
		println(s"주어진 데이터에는 일요일이 ${numOfSunday}개 들어 있습니다.")
	} finally {
		sc.stop()
	}
}
}
