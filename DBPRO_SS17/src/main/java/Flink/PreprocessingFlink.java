
package Flink;

import freqitems.util.ItemSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import freqitems.util.ItemSet;

import static java.nio.file.Files.*;


/**
 * @author Ariane Ziehn
 *         <p>
 *         Run with:
 *         ./OnlineRetail/OnlineRetail.csv 2 5
 */
public class PreprocessingFlink {
	public static  DataSet<Tuple2<Integer, String>> itemsSet;
	public static DataSet<List<Integer>> transactionListInteger;
	public static  DataSet<List<String>> transactionListString;

        

    
    public static  DataSet<List<Integer>> preprocessingFlink(String dataPath) throws Exception{
    	 final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

         /**
          * READ IN AND CLEAR DATA
          */
      /*
       * 1. Check how our data is organized:
       * InvoiceNo,StockCode,Description,
       * Quantity,InvoiceDate,UnitPrice,CustomerID,Country we have 8 cloumns >
       * .includeFields("00000000") the seperator is "," >
       * .fieldDelimiter(",") which columns are interesting? 1, and 2 > >
       * .includeFields("11000000") ATTENTION: in this data certain rows
       * contains missings, that is why we need to filter
       */
        DataSet<Tuple2<String, String>> csvInput
                 = env
                 .readCsvFile(dataPath).fieldDelimiter(",")
                 .includeFields("11000000").ignoreFirstLine()
                 .types(String.class, String.class)
                 .filter(new ReadInFilter());



        /*
		 * We have created a Flink dataset from your input data file with the
         * following informations: (TransactionID, ProduktID, TODO)
		 *
		 * To fasten up the mining process in the next step we map the String
		 * ProduktID to an unique numerical identifier
		 */
        
         itemsSet = csvInput.distinct(1)
                 .map(new AssignUniqueId())
                 .setParallelism(1);
     

      /*
       * In the next step we use dataset itemset above to create a List of
       * Integers containing all items of one transaction in a group of InvoiceNo > .
       * group(0) to map the unique identifiers
       * from itemSet to the String values from csvInput we can use the method
       * from BasketAnalysis, but need to edit it for time relevant sorting
       */


          transactionListInteger = csvInput.groupBy(0)
                .reduceGroup(new GroupItemsInListPerBill())
                .withBroadcastSet(itemsSet, "itemSet");
         

          transactionListString = csvInput.groupBy(0)
                 .reduceGroup(new GroupItemsInListPerBill2())
                .withBroadcastSet(itemsSet, "itemSet");
         
         
         
     
       return transactionListInteger;
    	
    }



    public static class GroupItemsInListPerBill
            extends
            RichGroupReduceFunction<Tuple2<String, String>, List<Integer>> {

        private static final long serialVersionUID = 1L;
        // get the set of items or products and create a hash map, so to map
        // product codes into integers
        private Collection<Tuple2<Integer, String>> itemSet; // these is the set
        // of possible
        // items or
        // products
        private Hashtable<String, Integer> itemSetMap = new Hashtable<String, Integer>();;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.itemSet = getRuntimeContext().getBroadcastVariable("itemSet");
            // Tuple2<map integer, product code>
            for (Tuple2<Integer, String> item : itemSet) {
                // itemSetMap.put(key: product code, value: map integer);
                itemSetMap.put(item.f1, item.f0);
            }
            // System.out.println(itemSetMap.entrySet());
        }

        @Override
        public void reduce(
                Iterable<Tuple2<String, String>> transactions,
                Collector<List<Integer>> out) {

            // transactions contains all the transactions with the same id or
            // RPA_TNR number
            // collect here the products of all those transactions and map them
            // into integers
            List<Integer> list_items = new ArrayList<Integer>();

            // f0 f1 f2
            // Tuple3<transaction number, timestamp, product>
            for (Tuple2<String, String> trans : transactions) {
                if (itemSetMap.containsKey(trans.f1))
                    list_items.add(itemSetMap.get(trans.f1));
                // System.out.println("   " + trans.f0 + " " + trans.f2 + " " +
                // itemSetMap.get(trans.f2));
            }
            // System.out.println("   ----------------------");
            Collections.sort(list_items);
            out.collect(list_items);
        }
    }

    public static class GroupItemsInListPerBill2
            extends
            RichGroupReduceFunction<Tuple2<String, String>, List<String>> {

        private static final long serialVersionUID = 1L;
       
        private Collection<Tuple2<Integer, String>> itemSet;
       
        private Hashtable<String, Integer> itemSetMap = new Hashtable<String, Integer>();
        ;

        @Override
        public void open(Configuration parameters) throws Exception {
        	//get the integer-stockNo dictionary
        	RuntimeContext rc =getRuntimeContext();
            this.itemSet = rc.getBroadcastVariable("itemSet");

             // Tuple2<map integer, stockNo>
            for (Tuple2<Integer, String> item : itemSet) {
            	// Tuple2<stockNo, map integer>
                itemSetMap.put(item.f1, item.f0);
            }

        }

        @Override
        public void reduce(
                Iterable<Tuple2<String, String>> transactions,
                Collector<List<String>> out) {

            // transactions contains all the transactions with the same
            // InvoiceNo
            // collect here the products of all those transactions and map them
            // into integers
           
            List<String> items = new LinkedList<String>();
          

            // Tuple2 <transaction number, product>
            for (Tuple2<String, String> trans : transactions) {
           
                if (itemSetMap.containsKey(trans.f1))

                    items.add(trans.f1);
            }


            if (!items.isEmpty())
                Collections.sort(items);
                out.collect(items);
        }
    }

   

    public static final class AssignUniqueId
            implements
            MapFunction<Tuple2<String, String>, Tuple2<Integer, String>> {

        private static final long serialVersionUID = 1L;
        int numItems = 1;

        @Override
        public Tuple2<Integer, String> map(Tuple2<String, String> value) {

            return new Tuple2<>(numItems++, value.f1);
        }
    }


    public static final class ReadInFilter implements
            FilterFunction<Tuple2<String, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(Tuple2<String, String> value) {
            // System.out.println("f0: "+ value.f0+ "f1: "+ value.f1+"f2: "+ value.f2);
            if (value.f0 != null && value.f0 != "")
                if (value.f1 != null && value.f1 != "")
                  //  if (value.f1.matches("[0-9:]+"))
                        return true;
            return false;
        }
    }

    public static final class IntegerFilter implements
            FilterFunction<Tuple2<String, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(Tuple2<String, String> value) {
            // System.out.println("f0: "+ value.f0+ "f1: "+ value.f1+"f2: "+ value.f2);
            if (value.f0 != null && value.f0 != "")
                if (value.f1 != null && value.f1 != "")
                    //  if (value.f1.matches("[0-9:]+"))
                    return true;
            return false;
        }
    }
}

