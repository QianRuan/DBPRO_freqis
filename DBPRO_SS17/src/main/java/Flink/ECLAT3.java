package Flink;

import freqitems.util.ItemSet;
import freqitems.util.Tree;
import freqitems.util.TreeNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Ariane
 */
public class ECLAT3 {

    private static int num_transaction = -1;

    public static DataSet<ItemSet> mine (DataSet<List<Integer>> transactionList, int minSupport, int iterations) throws Exception {
       // String dataPath = args[0];
        // final int minSupport = Integer.parseInt(args[1]);
       // final int iterations = Integer.parseInt(args[2]);
       // final int numpara = Integer.parseInt(args[3]);
//		String dataPath = "src/main/java/ressources/SyntheticData-T10I6D1e+05.txt";
//		final int minSupport = 900;
//		final int iterations = 15;
		final int numpara = 4;
       // long startTime = System.currentTimeMillis();

       // final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
       // env.setParallelism(numpara);
        /**
         * get input data, splits per line and adds tid, removes all
         * transactions shorten than iterations
         */
       // DataSet<String> text = env.readTextFile(dataPath);

       /* DataSet<Tuple2<Integer, List<Integer>>> transaction = text
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, List<Integer>>>() {
                    private static final long serialVersionUID = 1L;

                    // private int num_transaction = -1; > is not working!!!
                    @Override
                    public void flatMap(String in,
                                        Collector<Tuple2<Integer, List<Integer>>> out)
                            throws Exception {
                        List<Integer> transactionList = new ArrayList<Integer>();
                        for (String pid : Arrays.asList(in.split(" "))) {
                            // TODO split is fix here
                            Integer pidInt = Integer.parseInt(pid);
                            if (!transactionList.contains(pidInt))
                                transactionList.add(pidInt);
                        }
                        if (transactionList.size() >= iterations) {
                            num_transaction++;
                            out.collect(new Tuple2<Integer, List<Integer>>(
                                    num_transaction, transactionList));
                        }
                    }
                });*/
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



        /**
         * gets all frequent singletons from the dataset (Apriori method)
         */
        DataSet<Integer> itemCounts = transaction
                .flatMap(new TransactionSplitter()).groupBy(0).sum(1)
                .filter(new ItemSetFilterTuple2(minSupport))
                .map(new FieldSelector());
        System.out.println("itemCounts.count()"+itemCounts.count());

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
            // .filter(new ItemSetFilterInt(minSupport));
            //System.out.println("it1.count()"+it1.count());
            DataSet<Tree> tree1 = it1.flatMap(
                    new TreeBuild(minSupport, iterations)).withBroadcastSet(
                    it1, "it1");
          //  System.out.println("tree1.count()"+tree1.count());

            if (iterations > 2) {

                DataSet<Tree> tree2 = tree1.flatMap(new TreeBuild2(minSupport));
                // .withBroadcastSet(it1, "it1");
                System.out.println("tree2.count()"+tree2.count());

                if (iterations > 3) {

                    DataSet<Tree> tree3 = tree2.flatMap(new TreeBuild3(
                            minSupport));
                    System.out.println("tree3.count()"+tree3.count());

                    if (iterations > 4) {
                        /**
                         * mines all itemset with k=5;
                         */
                        DataSet<Tree> tree4 = tree3.flatMap(new TreeBuild4(
                                minSupport));
                        System.out.println("tree4.count()"+tree4.count());

                        if (iterations > 5) {

                            IterativeDataSet<Tree> prevSet = tree4
                                    .iterate(iterations - 5);
                            System.out.println("prevSet.count()"+prevSet.count());
                            /**
                             * iterative creates the next bigger itemset
                             */
                            DataSet<Tree> itn = prevSet.flatMap(new TreeBuildn(minSupport));
                            //output should be DataSet<Itemset>
                            DataSet<Tree> itm = prevSet.closeWith(itn);
                            System.out.println("itm.count()"+itm.count());

                            DataSet<ItemSet> items5 = itm.flatMap(new TreeToItemSet());
                            return items5;

                        } else{
                            DataSet<ItemSet> items4 = tree4.flatMap(new TreeToItemSet());
                            return items4;
                        }
                    } else{
                        DataSet<ItemSet> items3 = tree3.flatMap(new TreeToItemSet());
                        return items3;
                    }
                } else{
                    DataSet<ItemSet> items2 = tree2.flatMap(new TreeToItemSet());
                    return items2;
                }
            } else{
                DataSet<ItemSet> items1 = tree1.flatMap(new TreeToItemSet());
                return items1;
            }
        } else{
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


        //long estimatedTime = System.currentTimeMillis() - startTime;
       // System.out.println("Estimated time (sec): " + estimatedTime / 1000.0);

    }// main
    public static class TreeToItemSet implements FlatMapFunction<Tree, ItemSet> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(Tree in, Collector<ItemSet> out) throws Exception {
            List<Integer> itemList = new ArrayList<Integer>();
            String treeString=in.toString();
            for (String id : Arrays.asList(treeString.substring(1,treeString.length()-2).split(", "))) {
                // TODO split is fix here
                if (!id.isEmpty()) {
                    Integer idInt = Integer.parseInt(id);
                    itemList.add(idInt);

                }

            }
            Collections.sort(itemList);     //the count not known
            out.collect(new ItemSet(itemList, 0));
        }
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

    public static final class TreeBuild extends
            RichFlatMapFunction<Tuple2<Integer, BitSet>, Tree> {

        private static final long serialVersionUID = 1L;
        private Integer minSupport;

        private List<Tuple2<Integer, BitSet>> items = new ArrayList<Tuple2<Integer, BitSet>>();

        public TreeBuild(int minSupport, int iterations) {
            super();
            this.minSupport = minSupport;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.items = getRuntimeContext().getBroadcastVariable("it1");

        }

        @Override
        public void flatMap(Tuple2<Integer, BitSet> value, Collector<Tree> out)
                throws Exception {
            Tree tree = new Tree(2);
            TreeNode child = new TreeNode(value.f0, value.f1, value.f1);

            for (Tuple2<Integer, BitSet> item : items) {
                TreeNode children = new TreeNode(item.f0, item.f1, item.f1);
                child.addChild(children, minSupport, 2);
            }

            if (!child.isLeaf()) {
                tree.root.addRootChild(child, minSupport);
                out.collect(tree);
            }
        }

    }

    public static final class TreeBuild2 implements FlatMapFunction<Tree, Tree> {

        private static final long serialVersionUID = 1L;
        private Integer minSupport;

        public TreeBuild2(int minSupport) {
            super();
            this.minSupport = minSupport;

        }

        @Override
        public void flatMap(Tree value, Collector<Tree> out) throws Exception {
            Tree tree = new Tree(3);
            ArrayList<TreeNode> leaves = new ArrayList<TreeNode>();
            for (TreeNode item : value.root.getChildren()) {
                leaves = item.getLeaf(2);
            }

            for (TreeNode leaf : leaves) {
                for (TreeNode item : leaves) {
                    TreeNode children = new TreeNode(item.getId(),
                            item.getTidListOrginal(), item.getTidListOrginal());
                    leaf.addChild(children, minSupport, 3);
                }

                if (!leaf.isLeaf()) {
                    TreeNode mainParent = leaf.getMainParent();
                    tree.root.addRootChild(mainParent, minSupport);
                }

            }
            if (!tree.root.getChildren().isEmpty())
                out.collect(tree);

        }

    }

    public static final class TreeBuild3 implements FlatMapFunction<Tree, Tree> {

        private static final long serialVersionUID = 1L;
        private Integer minSupport;

        public TreeBuild3(int minSupport) {
            super();
            this.minSupport = minSupport;

        }

        @Override
        public void flatMap(Tree value, Collector<Tree> out) throws Exception {
            Tree tree = new Tree(4);
            ArrayList<TreeNode> leaves = new ArrayList<TreeNode>();
            for (TreeNode item : value.root.getChildren()) {
                leaves = item.getLeaf(3);
            }

            for (TreeNode leaf : leaves) {
                for (TreeNode item : leaves) {
                    if (leaf.getId() == 283 && item.getId() == 346) {
                    }
                    TreeNode children = new TreeNode(item.getId(),
                            item.getTidListOrginal(), item.getTidListOrginal());
                    leaf.addChild(children, minSupport, 4);
                }

                if (!leaf.isLeaf() && !tree.root.getChildren().contains(leaf)) {
                    TreeNode mainParent = leaf.getMainParent();
                    tree.root.addRootChild(mainParent, minSupport);
                }

            }
            if (!tree.root.getChildren().isEmpty())
                out.collect(tree);

        }

    }

    public static final class TreeBuild4 implements FlatMapFunction<Tree, Tree> {

        private static final long serialVersionUID = 1L;
        private Integer minSupport;

        public TreeBuild4(int minSupport) {
            super();
            this.minSupport = minSupport;

        }

        @Override
        public void flatMap(Tree value, Collector<Tree> out) throws Exception {
            Tree tree = new Tree(5);
            ArrayList<TreeNode> leaves = new ArrayList<TreeNode>();
            for (TreeNode item : value.root.getChildren()) {
                leaves = item.getLeaf(4);
            }

            for (TreeNode leaf : leaves) {
                for (TreeNode item : leaves) {
                    TreeNode children = new TreeNode(item.getId(),
                            item.getTidListOrginal(), item.getTidListOrginal());
                    leaf.addChild(children, minSupport, 5);
                }

                if (!leaf.isLeaf()) {
                    TreeNode mainParent = leaf.getMainParent();
                    tree.root.addRootChild(mainParent, minSupport);
                }

            }
            if (!tree.root.getChildren().isEmpty())
                out.collect(tree);

        }

    }

    public static final class TreeBuildn implements FlatMapFunction<Tree, Tree> {

        private static final long serialVersionUID = 1L;
        private Integer minSupport;

        public TreeBuildn(int minSupport) {
            super();
            this.minSupport = minSupport;

        }

        @Override
        public void flatMap(Tree value, Collector<Tree> out) throws Exception {

            Tree tree = new Tree((value.getIteration()+1));
            ArrayList<TreeNode> leaves = new ArrayList<TreeNode>();
            for (TreeNode item : value.root.getChildren()) {
                leaves = item.getLeaf(value.getIteration());
            }

            for (TreeNode leaf : leaves) {
                for (TreeNode item : leaves) {
                    TreeNode children = new TreeNode(item.getId(), item.getTidListOrginal(),
                            item.getTidListOrginal());
                    leaf.addChild(children, minSupport, (value.getIteration()+1));
                }

                if (!leaf.isLeaf()) {
                    TreeNode mainParent = leaf.getMainParent();
                    tree.root.addRootChild(mainParent, minSupport);
                }

            }
            if (!tree.root.getChildren().isEmpty())
                out.collect(tree);

        }

    }

}
