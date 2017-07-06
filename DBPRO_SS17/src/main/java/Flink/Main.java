package Flink;

import Flink.EclatFlink.ItemSet2;
import freqitems.util.ItemSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.memory.MemoryManager;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
/**
 * 
 * @author QianRuan
 *
 */

public class Main {
public static void main(String[] args) throws Exception {
	
	//preprocessing

    PreprocessingFlink p =new PreprocessingFlink();
	DataSet<List<Integer>> transactionList=p.preprocessingFlink("./OnlineRetail/OnlineRetail.csv");

    writeToCSVTuple(p.itemsSet,"./OnlineRetail/preprocessingResultStringInteger_Flink.csv");
	writeToCSV(p.transactionListString,"./OnlineRetail/preprocessingResultString_Flink.csv");
	writeToCSV(p.transactionListInteger,"./OnlineRetail/preprocessingResultInteger_Flink.csv");

      /**
       * MINING PART_Apriori
       */
    //set parameters
	int minSupport = 800;
    int numIterations =2;
    //start time

    final long startTime=System.currentTimeMillis();
	DataSet<ItemSet> items = AprioriFlink.mine(PreprocessingFlink.transactionListInteger, minSupport, numIterations);




    items.print();
    System.out.println("Apriori Integer Result");
    final long estimatedTime = System.currentTimeMillis() - startTime;
    System.out.println("Apriori_Flink takes (sec): " + estimatedTime / 1000.0 );

    /**
     *
     * POST PROCESSING _APRIORI
     */
    DataSet<List<String>> transactionListMapped = new PostprocessingFlink().postprocessingFlink(items);
    transactionListMapped .print();
    System.out.println("Apriori String Result ");
    writeToCSV(transactionListMapped,"./OnlineRetail/aprioriResultString_Flink.csv");

     new MemoryManager(1000000, 2).shutdown();


}//end of main

    public static final class ReadInFilter implements
            FilterFunction<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(String value) {
            // System.out.println("f0: "+ value.f0+ "f1: "+ value.f1+"f2: "+ value.f2);
            if (!value.isEmpty() )
                return true;
            return false;
        }
    }

public static <E> void writeToCSV(DataSet<List<E>> ds, String csvPath) throws Exception{
	 List<List<E>> ll = new LinkedList<List<E>>();
			 ll=ds.collect();
   
     StringBuilder sb = new StringBuilder();

     for (List<E> ls : ll) {
         String listString = ls.toString();
         sb.append(listString.substring(1, listString.length()-1));
         sb.append("\n");
     }
     try{
         PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
         writer.println(sb.toString().replace(",", ""));
         writer.close();
     } catch (IOException e) {
     }
}


public static <E> void writeToCSVItemSet(DataSet<ItemSet> ds, String csvPath) throws Exception{
	 List<ItemSet> ll = new LinkedList<ItemSet>();
	 ll=ds.collect();
    StringBuilder sb = new StringBuilder();

    for (ItemSet ls : ll) {
    	List<Integer> l = ls.getItemSetList();
        String listString = l.toString();
        sb.append(listString.substring(1, listString.length()-1));
        sb.append("\n");
    }
    try{
        PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
        writer.println(sb.toString().replace(",", " "));
        writer.close();
    } catch (IOException e) {
    }
}
    public static <E> void writeToCSVItemSet2(DataSet<ItemSet2> ds, String csvPath) throws Exception {
        List<ItemSet2> ll = new LinkedList<ItemSet2>();
        ll = ds.collect();


        StringBuilder sb = new StringBuilder();

        for (ItemSet2 ls : ll) {
            List<Integer> l = ls.getItemSetList();
            String listString = l.toString();
            sb.append(listString.substring(1, listString.length() - 1));
            sb.append("\n");
        }
        try {
            PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
            writer.println(sb.toString().replace(",", " "));
            writer.close();
        } catch (IOException e) {
        }
    }


public static <E> void writeToCSVTuple(DataSet<Tuple2<Integer, String>> ds, String csvPath) throws Exception{
	 //List<Tuple2<Integer, String>> ll = ds.collect();
	 List<Tuple2<Integer, String>> ll = new LinkedList<Tuple2<Integer, String>>();
	 ll=ds.collect();
   StringBuilder sb = new StringBuilder();

   for (Tuple2<Integer, String> ls : ll) {
       String listString = ls.toString();
       sb.append(listString.substring(1, listString.length()-1));
       sb.append("\n");
   }
   try{
       PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
       writer.println(sb.toString());//.replace(",", " "));
       writer.close();
   } catch (IOException e) {
   }
}

    public static <E> void writeToCSVTuple3(DataSet<Tuple3<Integer, Integer,Integer>> ds, String csvPath) throws Exception{
        //List<Tuple2<Integer, String>> ll = ds.collect();
        List<Tuple3<Integer, Integer,Integer>> ll = new ArrayList<>();
        ll=ds.collect();
        StringBuilder sb = new StringBuilder();

        for (Tuple3<Integer,Integer,Integer> ls : ll) {
            String listString = ls.toString();

            sb.append( ls.f0.toString()+" "+ ls.f1.toString());
            sb.append("\n");
        }
        try{
            PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
            writer.println(sb.toString().replace(",", " "));
            writer.close();
        } catch (IOException e) {
        }
    }
    public static <E> void writeToCSVInteger(DataSet<Integer> ds, String csvPath) throws Exception{
        //List<Tuple2<Integer, String>> ll = ds.collect();
        List<Integer> ll = new ArrayList<>();
        ll=ds.collect();
        StringBuilder sb = new StringBuilder();

        for (Integer ls : ll) {
           // String listString = ls.toString();

            sb.append( ls.toString());
            sb.append("\n");
        }
        try{
            PrintWriter writer = new PrintWriter(csvPath, "UTF-8");
            writer.println(sb.toString().replace(",", " "));
            writer.close();
        } catch (IOException e) {
        }
    }
}
