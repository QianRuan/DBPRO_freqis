package Flink;

import freqitems.util.ItemSet;
import org.apache.flink.api.java.DataSet;

import java.util.List;

/**
 * Created by KlaraRuanQian on 2017/7/2.
 */
public class MainEclat {
    public static void main(String[] args) throws Exception {
        //preprocessing
        PreprocessingFlink p =new PreprocessingFlink();

        DataSet<List<Integer>> transactionList=p.preprocessingFlink("./OnlineRetail/OnlineRetail.csv");
        /**
         * MINING PART_eclat
         */
        int minSupport = 800;
        int numIterations =2;

        long startTime = System.currentTimeMillis();
        DataSet<ItemSet> items= ECLAT3.mine(p.transactionListInteger, minSupport, numIterations);



        items.print();
        System.out.println("Eclat Integer Result");
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Eclat_Flink takes (sec): " + estimatedTime / 1000.0 );
        /**
         * Postprocessing PART_eclat
         */
        DataSet<List<String>> transactionListMapped2 = new PostprocessingFlink().postprocessingFlink(items);
        transactionListMapped2 .print();
        System.out.println("Eclat String Result ");
        Main.writeToCSV(transactionListMapped2,"./OnlineRetail/eclatResultString_Flink.csv");

        //new MemoryManager(1000000, 2).shutdown();


    }

}
