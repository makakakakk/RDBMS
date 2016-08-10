package m1;



class BTreeLeafNode<TKey extends Comparable<TKey>, TPointer> extends BTreeNode<TKey> {
	protected final static int LEAFORDER =  Integer.parseInt(DBApp.configReader("BPlusTreeN"));

	//msh dy mafroud e7na el handakhalha? 
	
	private Object[] values;
	
	public BTreeLeafNode() {
		this.keys = new Object[LEAFORDER + 1];
		this.values = new Object[LEAFORDER + 1];
	}
	//n + 1 3ala asas en el 4 dy starts from 0 wala eeeeeeh? xD 

	@SuppressWarnings("unchecked")
	public TPointer getValue(int index) {
		return (TPointer)this.values[index];
	}

	public void setValue(int index, TPointer value) {
		this.values[index] = value;
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}
	
	//el key 3obara 3an eh aslan? lol 
	
	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			 int cmp = this.getKey(i).compareTo(key);
			 if (cmp == 0) {
				 return i;
			 }
			 else if (cmp > 0) {
				 return -1;
			 }
		}
		
		return -1;
	}
	
	
	/* The codes below are used to support insertion operation */
	
	public void insertKey(TKey key, TPointer value) {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;
		this.insertAt(index, key, value);
	}
	
	private void insertAt(int index, TKey key, TPointer value) {
		// move space for the new key
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setValue(i + 1, this.getValue(i));
		}
		
		// insert new key and value
		this.setKey(index, key);
		this.setValue(index, value);
		++this.keyCount;
	}
	
	
	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		int midIndex = this.getKeyCount() / 2;
		
		BTreeLeafNode<TKey, TPointer> newRNode = new BTreeLeafNode<TKey, TPointer>();
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));
			newRNode.setValue(i - midIndex, this.getValue(i));
			this.setKey(i, null);
			this.setValue(i, null);
		}
		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;
		
		return newRNode;
	}
	
	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}
	
	
	
	
	/* The codes below are used to support deletion operation */
	
	public boolean delete(TKey key) {
		int index = this.search(key);
		if (index == -1)
			return false;
		
		this.deleteAt(index);
		return true;
	}
	
	private void deleteAt(int index) {
		int i = index;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setValue(i, this.getValue(i + 1));
		}
		this.setKey(i, null);
		this.setValue(i, null);
		--this.keyCount;
	}
	
	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Notice that the key sunk from parent is be abandoned. 
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
		BTreeLeafNode<TKey, TPointer> siblingLeaf = (BTreeLeafNode<TKey, TPointer>)rightSibling;
		
		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));
			this.setValue(j + i, siblingLeaf.getValue(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();
		
		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey, TPointer> siblingNode = (BTreeLeafNode<TKey, TPointer>)sibling;
		
		this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
		siblingNode.deleteAt(borrowIndex);
		
		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}
}
