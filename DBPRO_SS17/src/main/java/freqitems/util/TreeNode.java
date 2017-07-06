package freqitems.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class TreeNode {

    private Integer id;
    private BitSet tidList = new BitSet();
    private BitSet tidListOrginal = new BitSet();
    private TreeNode parent;
    private List<TreeNode> children = new ArrayList<TreeNode>();
    private boolean isLeaf;
    private int level;

    public TreeNode() {
        this.id = -1;
        this.tidList = new BitSet();
        this.isLeaf = false;
    }

    public TreeNode(Integer f0, BitSet f1) {
        this.id = f0;
        this.tidList.or(f1);
        this.isLeaf = true;
    }

    public TreeNode(Integer f0) {
        this.id = f0;

    }

    public TreeNode(Integer f0, BitSet f1, BitSet f2) {
        this.id = f0;
        this.tidList.or(f1);
        this.tidListOrginal.or(f2);
        this.isLeaf = true;
    }

    public BitSet getTidListOrginal() {
        return tidListOrginal;
    }

    public void setTidListOrginal(BitSet tidListOrginal) {
        this.tidListOrginal = tidListOrginal;
    }

    public void addChild(TreeNode child, int minSupport, int level) {

        if (this.id < child.id && !this.getChildrenId().contains(child.id)) {
            child.tidList.and(this.tidList);
            if (child.tidList.cardinality() >= minSupport) {
                this.children.add(child);
                this.isLeaf = false;
                child.isLeaf = true;
                child.parent = this;
                child.level = level;
            }
        }

    }

    public List<Integer> getChildrenId() {
        List<Integer> id = new ArrayList<Integer>();
        for(TreeNode node : this.getChildren()){
            if(!id.contains(node.id))
                id.add(node.id);
        }
        return id;
    }

    private void addChildLevel(TreeNode child) {

        this.children.add(child);
        this.isLeaf = false;
        child.parent = this;

    }

    public void addRootChild(TreeNode child, int minSupport) {
        if (!this.getChildrenId().contains(child.id)) {
            this.children.add(child);
            child.parent = this;
            child.level = 1;
        } else {
            TreeNode duplicate = this.children
                    .get(this.children.indexOf(child));
            checkTree(duplicate, child);
        }
    }

    private void checkTree(TreeNode duplicate, TreeNode child) {
        for (TreeNode item : child.children) {

            int i = duplicate.children.indexOf(item);
            if (i < 0) {
                duplicate.addChildLevel(item);
            } else {
                level++;
                TreeNode dupl = duplicate.children.get(i);
                checkTree(dupl, item);
            }
        }
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public TreeNode getParent() {
        return parent;
    }

    public void setParent(TreeNode parent) {
        this.parent = parent;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }

    public ArrayList<TreeNode> getLeaf(int level) {
        ArrayList<TreeNode> list = new ArrayList<TreeNode>();
        if (this.isLeaf && this.level == level && !list.contains(this)) {
            list.add(this);
            return list;
        } else {
            for (TreeNode item : this.children) {
                if (item.isLeaf && item.level == level && !list.contains(item)){
                    list.add(item);
                }else {
                    checkLeaf(item, list, level);
                }
            }
            return list;
        }
    }

    public BitSet getTidList() {
        return tidList;
    }

    public void setTidList(BitSet tidList) {
        this.tidList = tidList;
    }

    private void checkLeaf(TreeNode item, ArrayList<TreeNode> list, int level) {

        for (TreeNode it : item.children) {
            if (it.isLeaf && it.level == level && !list.contains(it)){
                list.add(it);
            }else
                checkLeaf(it, list, level);
        }

    }

    @Override
    public String toString() {
        return this.id.toString();
    }

    public TreeNode getMainParent() {
        if (this.parent.id == -1) {
            return this;
        } else {
            TreeNode dummy = this.parent;

            while (dummy.parent.id != -1) {
                dummy = dummy.parent;
            }
            return dummy;
        }

    }

//	public TreeNode getMainParent() throws CloneNotSupportedException {
//		if (this.parent.id == -1) {
//			return this;
//		} else {
//			TreeNode dummy = this.parent;
//			TreeNode child = this;
//			while (dummy.parent.id != -1) {
//				child = dummy;
//				dummy = dummy.parent;
//
//			}
//
//			dummy.children.clear();
//			dummy.children.add(child);
//			return dummy;
//		}
//
//	}

//	public TreeNode getMainParent() {
//		TreeNode dummy1 = this;
//		//sets.add(this);
//		if (this.parent.id == -1) {
//			return this;
//		} else {
//			TreeNode dummyReal = this.parent;
//			TreeNode dummy = new TreeNode(this.parent.id);
//			dummy.children.add(this);
//			this.parent=dummy;
//
//			while (dummyReal.parent.id != -1) {
//
//				dummy1.children.add(dummy);
//				dummy.parent=dummy1;
//				//sets.add(dummy1);
//				dummy1 = new TreeNode(dummy1.parent.id);
//				dummyReal = dummyReal.parent;
//
//			}
//			//Collections.sort(sets);
//			return dummy1;
//		}
//
//	}

    public ArrayList<Integer> getItemSet() {
        ArrayList<Integer> sets = new ArrayList<Integer>();
        sets.add(0, this.id);
        if (this.parent.id == -1) {
            return sets;
        } else {
            TreeNode dummy = this.parent;
            sets.add(1, dummy.id);
            while (dummy.parent.id != -1) {
                sets.add(dummy.parent.id);
                dummy = dummy.parent;

            }
            Collections.sort(sets);
            return sets;
        }

    }

}
