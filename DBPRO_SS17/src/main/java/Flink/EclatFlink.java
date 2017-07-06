package Flink;

import freqitems.util.ItemSet;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * 
 * @author QianRuan
 *
 */


public class EclatFlink {


public static class ItemSet2 {

		public List<Integer> itemset = new ArrayList<Integer>();
		public BitSet bits = new BitSet();


		public ItemSet2(List<Integer> args, BitSet arg0) {
			this.itemset = args;
			this.bits.or(arg0);
		}

		public String getId() {
			return this.itemset.toString();
		}

		@Override
		public String toString() {
			return this.itemset.toString();
		}

		public List<Integer> getItemSetList() {
			return this.itemset;
		}


	}

	private static int num_transaction = -1;


	/**
	 * Main
	 * @throws Exception
	 */

	public static DataSet<ItemSet> mine (DataSet<List<Integer>> transactionList, int minSupport, int iterations) throws Exception {


		//final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		/**
		 * get input data, splits per line and adds tid
		 */

		// transactionId & Itemset
		DataSet<Tuple2<Integer, List<Integer>>> transaction = transactionList
				.flatMap(new FlatMapFunction<List<Integer>, Tuple2<Integer, List<Integer>>>() {
							 private static final long serialVersionUID = 1L;


							 @Override
							 public void flatMap(List<Integer> in, Collector<Tuple2<Integer, List<Integer>>> out) throws Exception {

								// if (in.size() > iterations) {
									 num_transaction++;
									 out.collect(new Tuple2<Integer, List<Integer>>(num_transaction, in));

								// }
							 }
						 });

	//	long startTime = System.currentTimeMillis();


		/**
		 * gets all frequent singletons from the dataset (Apriori method)
		 */

//		DataSet<Tuple2<Integer, Integer>> itemSetsIterationTuple2 = transactionList
//				.flatMap(new TransactionSplitter()).groupBy(0).sum(1)
//				.filter(new ItemSetFilterTuple2(minSupport));


		DataSet<Integer> itemCounts = transaction
				.flatMap(new TransactionSplitter())
				.groupBy(0).sum(1)
				.filter(new ItemSetFilterTuple2(minSupport))
				.map(new FieldSelector());
		/*
		DataSet<Tuple2<Integer, Integer>> itemCounts1 = transaction
				.flatMap(new TransactionSplitter());
		itemCounts1.print();

		System.out.println("1______"+itemCounts1.count());
		DataSet<Tuple2<Integer, Integer>>itemCounts2=itemCounts1.groupBy(0).sum(1);
		itemCounts2.print();

		System.out.println("2______"+itemCounts2.count());

		DataSet<Integer> itemCounts=	itemCounts2
				.filter(new ItemSetFilterTuple2(minSupport))
				.map(new FieldSelector());*/

		if (iterations > 1) {

			/**
			 * CHANGE TO ECLAT this method gets all singletons from previous
			 * DataSet "itemCounts" and, using the DataSet "transaction" as
			 * Broadacast, creates the tidlist (tid) for the ECLAT for each
			 * frequent item (pid) it1 = iteration 1
			 */
			DataSet<Tuple2<Integer, BitSet>> it1 = itemCounts
					.flatMap(
							new RichFlatMapFunction<Integer, Tuple2<Integer, BitSet>>() {
								private static final long serialVersionUID = 1L;
								private Collection<Tuple2<Integer, List<Integer>>> transaction;

								@Override
								public void open(Configuration parameters)
										throws Exception {
									this.transaction = getRuntimeContext()
											.getBroadcastVariable("transaction");
								}

								@Override
								public void flatMap(Integer pid,
													Collector<Tuple2<Integer, BitSet>> out)
										throws Exception {
									BitSet tid = new BitSet();
									for (Tuple2<Integer, List<Integer>> list : transaction) {
										if (list.f1.indexOf(pid) != -1) {
											tid.set(list.f0);
										}
									}
									out.collect(new Tuple2<Integer, BitSet>(
											pid, tid));
								}
							}).withBroadcastSet(transaction, "transaction");
			//.filter(new ItemSetFilterInt(minSupport));
			//it1.print();
			//System.out.println("there is it1_"+it1.count());

			        DataSet<Tuple3<Integer, Integer, BitSet>> it2 = it1
					.combineGroup(new CandidateGeneration2(minSupport));
			//it2.print();
			//System.out.println("there is it2_"+it2.count());
			//it2.print();

			if (iterations > 2) {

				DataSet<ItemSet2> it3 = it2
						.combineGroup(new CandidateGeneration3(minSupport))
						.distinct(new ItemSetKey());
				//.distinct(0, 1, 2);
				//it3.print();
				//System.out.println("there is it3_"+it3.count());
//             
//				if (iterations > 3) {
//
//					DataSet<ItemSet> it4 = it3							
//							.combineGroup(
//							new CandidateGeneration4(minSupport))
//							.distinct(new ItemSetKey());
//							//new ItemSetKey4());
				if (iterations - 3 >= 1) {
					IterativeDataSet<ItemSet2> prevSet = it3
							.iterate(iterations - 3);

					/**
					 * iterative creates the next bigger itemset
					 */
					DataSet<ItemSet2> itn = prevSet.combineGroup(
							new CandidateGeneration(minSupport)).distinct(
							new ItemSetKey());
					DataSet<ItemSet2> m = prevSet.closeWith(itn);

					DataSet<ItemSet> items4 = m.flatMap(new FlatMapFunction<ItemSet2, ItemSet>() {
						private static final long serialVersionUID = 1L;

						@Override
						public void flatMap(ItemSet2 in, Collector<ItemSet> out) throws Exception {
							List<Integer> itemList = new ArrayList<Integer>();
							for (Integer i : in.getItemSetList())
								itemList.add(i);
							Collections.sort(itemList);
							out.collect(new ItemSet(itemList, 0));
						}
					});
					//prevSet.closeWith(itn).print(); //!!!
					//System.out.println("there is 4 prevSet.closeWith(itn).print();"); //4
					return items4;
					//Main.writeToCSVItemSet2(prevSet.closeWith(itn),"./OnlineRetail/eclatResultInteger_Flink.csv");

//					} else
//						it4.print();

				} else {
					DataSet<ItemSet> items3 = it3.flatMap(new FlatMapFunction<ItemSet2, ItemSet>() {
						private static final long serialVersionUID = 1L;

						@Override
						public void flatMap(ItemSet2 in, Collector<ItemSet> out) throws Exception {
							List<Integer> itemList = new ArrayList<Integer>();
							for (Integer i : in.getItemSetList())
								itemList.add(i);
							Collections.sort(itemList);
							out.collect(new ItemSet(itemList, 0));
						}
					});
					//System.out.println("there is 3 "+items3.count());
					//System.out.println(items3.count());
					return items3;
					//Main.writeToCSVItemSet2(it3,"./OnlineRetail/eclatResultInteger_Flink.csv");
					//it3.print();
					//System.out.println("there is it3.print()"); //3
				   } }else{

					        DataSet<ItemSet> items2 = it2.map(new FieldSelector3())
							.flatMap(new FlatMapFunction<Tuple3<Integer, Integer, Integer>, ItemSet>() {
								private static final long serialVersionUID = 1L;

								@Override
								public void flatMap(Tuple3<Integer, Integer, Integer> in, Collector<ItemSet> out) throws Exception {
									List<Integer> itemList = new ArrayList<Integer>();
									itemList.add(in.f0);
									itemList.add(in.f1);

									//Collections.sort(itemList);
									out.collect(new ItemSet(itemList, in.f2));
								}
							});
				//System.out.println("there is 2");
					return items2;
					//

					//Main.writeToCSVTuple3(it2.map(new FieldSelector3()), "./OnlineRetail/eclatResultInteger_Flink.csv");
					//	it2.map(new FieldSelector3()).print();
					//System.out.println("there is it2.map(new FieldSelector3()).print();");//2

				} }else{

				//	itemCounts.print();
				//	System.out.println("Elcat Integer Result");
					//System.out.println("there is itemCounts.print();"); //1
					//Main.writeToCSVInteger(itemCounts, "./OnlineRetail/eclatResultInteger_Flink.csv");

					//long estimatedTime = System.currentTimeMillis() - startTime;
					//System.out.println("ECLAT_Flink takes (sec): " + estimatedTime / 1000.0 + " to mine and write");
					DataSet<ItemSet> items = itemCounts
							.flatMap(new FlatMapFunction<Integer, ItemSet>() {
								private static final long serialVersionUID = 1L;

								@Override
								public void flatMap(Integer in, Collector<ItemSet> out) throws Exception {
									List<Integer> itemList = new ArrayList<Integer>();
									itemList.add(in);
									Collections.sort(itemList);
									out.collect(new ItemSet(itemList, 0));
								}
							});
			//System.out.println("there is 1 ");
					return items;
				}
			}


	// main

	public static final class ItemSetFilterInt implements
			FilterFunction<Tuple2<Integer, BitSet>> {
		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public ItemSetFilterInt(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public boolean filter(Tuple2<Integer, BitSet> value) {
			return value.f1.cardinality() >= minSupport;
		}
	}

	public static final class CandidateGeneration implements
			GroupCombineFunction<ItemSet2, ItemSet2> {

		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public void combine(Iterable<ItemSet2> arg0, Collector<ItemSet2> arg1)
				throws Exception {
			List<ItemSet2> l1 = new ArrayList<ItemSet2>();
			for (ItemSet2 i : arg0) {
				l1.add(i);
			}
			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {
					List<Integer> key = new ArrayList<Integer>();

					key.addAll(l1.get(i).itemset);
					int counter = 0;
					for (Integer b : l1.get(j).itemset) {

						if (!key.contains(b)) {
							counter++;
							if (counter >= 2)
								break;
							else
								key.add(b);
						}
					}

					if (counter == 1) {

						BitSet tid = (BitSet) l1.get(i).bits.clone();
						(tid).and(l1.get(j).bits);

						if (tid.cardinality() >= minSupport) {
							Collections.sort(key);
							arg1.collect(new ItemSet2(key, tid));
						}
					}
				}// out if
			}
		}// out for
	}

	public static class TransactionSplitter
			implements
			FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, Integer>> {
		/**
		 * returns each item of one transaction with a count of 1
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<Integer, List<Integer>> transaction,
				Collector<Tuple2<Integer, Integer>> out) {
			for (Integer item : transaction.f1) {
				out.collect(new Tuple2<Integer, Integer>(item, 1));
			}
		}
	}

	public static final class ItemSetFilterTuple2 implements
			FilterFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public ItemSetFilterTuple2(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public boolean filter(Tuple2<Integer, Integer> value) {
			return value.f1 >= minSupport;

		}
	}

	public static final class FieldSelector implements
			MapFunction<Tuple2<Integer, Integer>, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f0;
		}
	}

	public static final class FieldSelector4
			implements
			MapFunction<Tuple4<Integer, Integer, Integer, BitSet>, Tuple4<Integer, Integer, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(
				Tuple4<Integer, Integer, Integer, BitSet> value) {
			return new Tuple4<Integer, Integer, Integer, Integer>(value.f0,
					value.f1, value.f2, value.f3.cardinality());
		}
	}

	public static final class FieldSelector3
			implements
			MapFunction<Tuple3<Integer, Integer, BitSet>, Tuple3<Integer, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer, Integer, Integer> map(
				Tuple3<Integer, Integer, BitSet> value) {
			return new Tuple3<Integer, Integer, Integer>(value.f0, value.f1,
					value.f2.cardinality());
		}
	}

	public static class ItemSetKey implements KeySelector<ItemSet2, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(ItemSet2 i) throws Exception {

			return i.getId();
		}

	}

	public static final class CandidateGeneration2
			implements
			GroupCombineFunction<Tuple2<Integer, BitSet>, Tuple3<Integer, Integer, BitSet>> {
		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration2(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public void combine(Iterable<Tuple2<Integer, BitSet>> arg0,
				Collector<Tuple3<Integer, Integer, BitSet>> out)
				throws Exception {

			List<Tuple2<Integer, BitSet>> l1 = new ArrayList<Tuple2<Integer, BitSet>>();
			for (Tuple2<Integer, BitSet> i : arg0) {
				l1.add(i);
			}

			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {
					int k1 = l1.get(i).f0;
					int k2 = l1.get(j).f0;
					BitSet tid = (BitSet) l1.get(i).f1.clone();
					(tid).and(l1.get(j).f1);

					if (tid.cardinality() >= minSupport) {
						if (k1 < k2)
							out.collect(new Tuple3<Integer, Integer, BitSet>(
									k1, k2, tid));
						if (k1 > k2)
							out.collect(new Tuple3<Integer, Integer, BitSet>(
									k2, k1, tid));
					}
				}
			}
		}// out for
	}

	public static final class CandidateGeneration3
			implements
			GroupCombineFunction<Tuple3<Integer, Integer, BitSet>, ItemSet2> {

		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration3(int minSupport) {
			super();
			this.minSupport = minSupport;
			//System.out.println(this.minSupport);
		}

		@Override
		public void combine(Iterable<Tuple3<Integer, Integer, BitSet>> arg0,
				Collector<ItemSet2> out)
				throws Exception {

			List<Tuple3<Integer, Integer, BitSet>> l1 = new ArrayList<Tuple3<Integer, Integer, BitSet>>();
			for (Tuple3<Integer, Integer, BitSet> i : arg0) {
				l1.add(i);
			}

			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {

//					if (l1.get(i).f0 == l1.get(j).f0
//							|| l1.get(i).f1 == l1.get(j).f1
//							|| l1.get(i).f1 == l1.get(j).f0
//							|| l1.get(i).f0 == l1.get(j).f1) {

						List<Integer> key = new ArrayList<Integer>();
						key.add(0, l1.get(i).f0);
						key.add(1, l1.get(i).f1);
						int counter = 0; 
						if (!key.contains(l1.get(j).f0)) {
							key.add(2, l1.get(j).f0);
							counter++;
						} else {
							if (!key.contains(l1.get(j).f1)) {
								key.add(2, l1.get(j).f1);
								counter++;
							}
						}
						if (counter == 1) {
							BitSet tid = (BitSet) l1.get(i).f2.clone();
							(tid).and(l1.get(j).f2);
							if (tid.cardinality() >= minSupport) {
								Collections.sort(key);
								out.collect(new ItemSet2(
										key, tid));
							}
						}
					//}// if
				}
			}
		}// combine
	}

//	public static final class ItemSetKey4 implements
//			KeySelector<ItemSet, Tuple4<Integer, Integer, Integer, Integer>> {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public Tuple4<Integer, Integer, Integer, Integer> getKey(ItemSet i)
//				throws Exception {
//			Collections.sort(i.itemset);
//			return i.getTuple4();
//		}

//	}

	public static final class CandidateGeneration4
			implements
			GroupCombineFunction<Tuple4<Integer, Integer, Integer, BitSet>, ItemSet2> {

		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration4(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public void combine(
				Iterable<Tuple4<Integer, Integer, Integer, BitSet>> arg0,
				Collector<ItemSet2> out) throws Exception {

			List<Tuple4<Integer, Integer, Integer, BitSet>> l1 = new ArrayList<Tuple4<Integer, Integer, Integer, BitSet>>();
			for (Tuple4<Integer, Integer, Integer, BitSet> i : arg0) {
				l1.add(i);
			}
			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {
					List<Integer> key = new ArrayList<Integer>();

					key.add(l1.get(i).f0);
					key.add(l1.get(i).f1);
					key.add(l1.get(i).f2);

					int counter = 0;
					if (!key.contains(l1.get(j).f0)) {
						key.add(l1.get(j).f0);
						counter++;
					}
					if (!key.contains(l1.get(j).f1)) {
						key.add(l1.get(j).f1);
						counter++;
					}
					if (!key.contains(l1.get(j).f2)) {
						key.add(l1.get(j).f2);
						counter++;
					}

					if (counter == 1) {

						BitSet tid = (BitSet) l1.get(i).f3.clone();
						(tid).and(l1.get(j).f3);

						if (tid.cardinality() >= minSupport) {
							Collections.sort(key);
							out.collect(new ItemSet2(key, tid));
						}
					}
				}// out if
			}
		}// out for
	}
}