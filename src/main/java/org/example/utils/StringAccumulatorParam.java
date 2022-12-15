package org.example.utils;

import org.apache.spark.util.AccumulatorV2;

/**
 * Used to extend the Spark accumulator feature, to pass processing time from
 * each worker node to master node. Can be read only on the master node, but
 * every worker can append a {@code String} to it.
 */
//String的累加器
public class StringAccumulatorParam extends AccumulatorV2<String,String> {

	private static final long serialVersionUID = -435454767041068637L;
	private String res = "";

	@Override
	public boolean isZero() {
		return "".equals(res);
	}

	@Override
	public AccumulatorV2 copy() {
		return null;
	}

	@Override
	public void reset() {
		res = "";
	}

	@Override
	public String value() {
		return res;
	}

	@Override
	public void merge(AccumulatorV2 other) {

	}

	@Override
	public void add(String v) {
		res = res + v;
	}

	public StringAccumulatorParam() {
	}

//	/**
//	 * Merge two accumulated values together. Is allowed to modify and return
//	 * the first value for efficiency (to avoid allocating objects).
//	 *
//	 * @param arg0
//	 *            one set of accumulated data
//	 * @param arg1
//	 *            another set of accumulated data
//	 * @return both data sets merged together
//	 */
//
//	@Override
//	public String addInPlace(String arg0, String arg1) {
//
//		return arg0 + arg1;
//	}
//
//	/**
//	 * Return the "zero" (identity) value for an accumulator type, given its
//	 * initial value. For example, if R was a vector of N dimensions, this would
//	 * return a vector of N zeroes.
//	 */
//	@Override
//	public String zero(String arg0) {
//
//		return new String();
//	}
//
//	/**
//	 * Add additional data to the accumulator value. Is allowed to modify and
//	 * return `r` for efficiency (to avoid allocating objects).
//	 *
//	 * @param arg0
//	 *            the current value of the accumulator
//	 * @param arg1
//	 *            the data to be added to the accumulator
//	 * @return the new value of the accumulator
//	 */
//	@Override
//	public String addAccumulator(String arg0, String arg1) {
//		return arg0 + arg1;
//	}

}