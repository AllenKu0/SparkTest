package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.example.http.SparkRefineHTTPClient;
import org.example.utils.RDDContentBody;
import org.json.JSONArray;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Function that is run on the worker node:<br>
 * 
 * @param header
 *            is the Spark broadcast variable, containing the original header
 * @param transformfile
 *            path to the JSON transform operations
 * @param accum
 *            Spark {@link AccumulatorV2}{@code<String>} used to append the worker
 *            processing time to it that will be read on the master.
 * @author andrey
 */

public class JobFunction implements FlatMapFunction<Iterator<String>, String>,
		Serializable {
	private static final long serialVersionUID = 1401955470896751634L;
	private Broadcast<String> header;
	private String transformFile;
	private AccumulatorV2<String,String> accum;

	public JobFunction(Broadcast<String> header, String transformFile,
					   AccumulatorV2<String,String> accum) {
		this.header = header;
		this.transformFile = transformFile;
		this.accum = accum;
	};

	/**
	 * Processes the passed chunk of data by submitting it to Refine, using
	 * {@link SparkRefineHTTPClient} and returning the transformed data.<br>
	 * Deals with the presence/absence of the header in obtained chunk:<br>
	 * - if a partition has a header, leave it. - if partition doesn't have a
	 * header, add it, and than remove it on transformed data.
	 *
	 * @param originalRDD {@code Iterator} over the chunk RDD
	 * @return transformed {@code List<String>} output from Refine
	 */

	@Override
	public Iterator<String> call(Iterator<String> originalRDD) throws Exception {
		List<String> transformed = null;

		long startTime = System.nanoTime();

		//類似HashMap 可以存對應值（key,value)
		Properties exporterProperties = new Properties();
		//輸出規則
		exporterProperties.setProperty("format", "csv");

		//轉換規則
		JSONArray transformArray = new JSONArray(
				FileUtils.readFileToString(new File(transformFile)));

		//Refine 運作預設
//		SparkRefineHTTPClient client = new SparkRefineHTTPClient("localhost",
//				3333);

		SparkRefineHTTPClient client = new SparkRefineHTTPClient("10.0.0.203",
				8080);

		//header.getValue()為50.csv的第一行
		RDDContentBody RDDchunk = new RDDContentBody(originalRDD,
				header.getValue());

		//開始執行轉換於refine並回傳結果（JSON轉成List）
		transformed = client.transform(RDDchunk, transformArray,
				exporterProperties);

		if (!RDDchunk.hadHeader())
			transformed.remove(0);

		//累加器內加入耗時
		accum.add(String.format("\t%2.3f",
				(System.nanoTime() - startTime) / 1000000000.0));

		return transformed.iterator();

	}

}