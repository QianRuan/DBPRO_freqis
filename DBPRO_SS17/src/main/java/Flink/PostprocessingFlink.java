package Flink;

import java.util.*;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import freqitems.util.ItemSet;

public class PostprocessingFlink{
	

	public DataSet<List<String>>  postprocessingFlink(DataSet<ItemSet> items){
	DataSet<Tuple2<Integer, String>>itemsSet= PreprocessingFlink.itemsSet;
	DataSet<List<String>> transactionListMapped = items.flatMap(
			new MapBackTransactions1()).withBroadcastSet(itemsSet,
			"itemSet");
	return transactionListMapped;



	}
    public static class MapBackTransactions1 extends
            RichFlatMapFunction<ItemSet, List<String>> {

        // get the set of items or products and create a hash map, so to map
        // product codes into integers
        private Collection<Tuple2<Integer, String>> itemSet; // these is the set
        // of possible
        // items or
        // products
        // here create the Hash map the other way around to map back
        private Hashtable<Integer, String> itemSetMap = new Hashtable<Integer, String>();;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.itemSet = getRuntimeContext().getBroadcastVariable("itemSet");
            // Tuple2<map integer, product code>
            for (Tuple2<Integer, String> item : itemSet) {
                // itemSetMap.put(key: product code, value: map integer);
                itemSetMap.put(item.f0, item.f1);
            }
        }

        @Override
        public void flatMap(ItemSet freqItems, Collector<List<String>> out) {

            List<String> list_items = new ArrayList<String>();
            // List<Integer> itemsList = freqItems.getItemSetList();
            for (Integer item : freqItems.getItemSetList()) {
                if (itemSetMap.containsKey(item))
                    list_items.add(itemSetMap.get(item));
            }
            out.collect(list_items);

        }
    }

}


