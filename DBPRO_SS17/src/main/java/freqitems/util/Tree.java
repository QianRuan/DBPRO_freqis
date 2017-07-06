package freqitems.util;

import java.util.ArrayList;


public class Tree {

    public TreeNode root;
    private int iteration;

    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public Tree(int iteration){
        this.root = new TreeNode();
        this.iteration = iteration;
    }

    public Tree(){
        this.root = new TreeNode();
        //this.iteration = iteration;
    }

    @Override
    public String toString(){
        //String item = "itemSets \n"+this.iteration;
        String item = "";
		/*
		for(TreeNode node : this.root.getChildren()){
			ArrayList<TreeNode> leaves = node.getLeaf(iteration);
			//System.out.println(leaves);
			for(TreeNode leaf : leaves){
				item += leaf.getItemSet().toString(); // + "\n";
			}
		}*/
        for(TreeNode node : this.root.getChildren()){
            ArrayList<TreeNode> leaves = node.getLeaf(iteration);
            //System.out.println(leaves);
            for(int i = 0; i<leaves.size(); i++){
                if(i==leaves.size()-1)
                    item += leaves.get(i).getItemSet().toString()+"\n";
                else
                    item += leaves.get(i).getItemSet().toString()+",";//
            }
        }

        return item;

    }


}