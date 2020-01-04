import java.util.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;


public class CloudBST {
	
	private String treeName;
	private BSTDynamoDBAccess dynamoDBAccess;
	private Map<String, CloudNode> cache;
	private String str; // temporary for traversals
	
	public CloudBST(String treeName) {
		this.treeName = treeName;
		dynamoDBAccess = new BSTDynamoDBAccess();
		dynamoDBAccess.createTable(treeName);
		cache = new HashMap<String, CloudNode>();
	}
	
	public CloudNode getNode(String nodeId) {
		
		if (cache.containsKey(nodeId)) { // first try the cache
			return cache.get(nodeId);
		}
		if (nodeId == null) {
			return null;
		}
		
		String cloudNodeData = null;
		String cloudNodeLeft = null;
		String cloudNodeRight = null;
		
		Map<String, AttributeValue> cloudNode = dynamoDBAccess.getNode(treeName, nodeId);
		if (cloudNode == null) {
			return null;
		} else {
			if (cloudNode.get("data") != null) {
				cloudNodeData = cloudNode.get("data").getS();
			}
			if (cloudNode.get("left") != null) {
				cloudNodeLeft = cloudNode.get("left").getS();
			}
			if (cloudNode.get("right") != null) {
				cloudNodeRight = cloudNode.get("right").getS();
			}
		}
		System.out.println("getNode data: " + cloudNodeData + ", left: " + cloudNodeLeft + ", right: " + cloudNodeRight);
		
		CloudNode newNode =  new CloudNode(nodeId, cloudNodeData, cloudNodeLeft, cloudNodeRight);
		cache.put(nodeId, newNode);
		return newNode;
	}
	/**
	 * 
	 * @param nodeId nodeId to update
	 * @param data new data
	 * @param left new left
	 * @param right new right
	 */
	public void updateNode(String nodeId, String data, String left, String right) {
		CloudNode nodeToUpdate = new CloudNode(nodeId, data, left, right);
		cache.put(nodeId, nodeToUpdate);
		dynamoDBAccess.updateItem(treeName, nodeId, data, left, right);
	}
	/**
	 * 
	 * @param nodeId nodeId to add
	 * @param data data to add
	 * @param left new node's left
	 * @param right new node's right
	 */
	public void addNode(String nodeId, String data, String left, String right) {
		CloudNode newNode = new CloudNode(nodeId, data, left, right);
		cache.put(nodeId, newNode);
		Map<String, AttributeValue> item = dynamoDBAccess.newNode(nodeId, data, left, right);
		dynamoDBAccess.addItemToTable(item, treeName);
	}
	
	public void insert(String data) {
		
		CloudNode root = getNode("root");
		if (root == null) {
			addNode("root", data, null, null);
		} else {
			insert(data, "root");
		}
		printCache();
	}
	
	private void insert(String data, String nodeId) {
		
		// query treeName table with nodeId, get node.data, node.left, node.right
		CloudNode cur = getNode(nodeId);
		
		if (data.compareTo(cur.data) < 0) {
			System.out.println("inserting " + data + " into left " + cur.left);
			
			if (cur.left == null || cur.left.equals("deleted")) {
				
				String leftId = UUID.randomUUID().toString();
				// link to new left node, then add
				updateNode(nodeId, cur.data, leftId, cur.right);
				addNode(leftId, data, null, null);
				
			} else {
				insert(data, cur.left);
			}
			
		} else if (data.compareTo(cur.data) >= 0) {
			System.out.println("inserting " + data + " into right " + cur.right);
			
			if (cur.right == null || cur.right.equals("deleted")) {
				
				String rightId = UUID.randomUUID().toString();
				// link to new right node, then add
				updateNode(nodeId, cur.data, cur.left, rightId);
				addNode(rightId, data, null, null);
				
			} else {
				insert(data, cur.right);
			}
		}
	}
	
	public void printCache() {
		for (String key : cache.keySet()) {
			CloudNode cloudNode = cache.get(key);
			System.out.println("CloudBST Cache: " + cloudNode);
		}
	}
	
	public boolean contains(String data) {
		CloudNode root = getNode("root");
		if (root == null) {
			return false;
		} else {
			return contains(data, root);
		}
	}
	
	private boolean contains(String data, CloudNode node) {
		
		if (node == null) {
			
		} else {
			if (node.data.compareTo(data) > 0) {
				return contains(data, getNode(node.left));
			} else if (node.data.compareTo(data) < 0) {
				return contains (data, getNode(node.right));
			} else if (node.data.equals(data)) {
				System.out.println("Found node: " + node);
				return true;
			}
		}
		return false;
	}
	
	public boolean delete(String data) {
		CloudNode root = getNode("root");
		if (root == null) {
			return false;
		} else {
			if (contains(data)) {
				
				if (root.data.equals(data)) {
					
//					removeNode(root);
//					return false;
					if (root.left != null && !root.left.equals("deleted")) {
						CloudNode largest = largest(getNode(root.left));
						delete(largest.data);
						updateNode(root.nodeId, largest.data, root.left, root.right);
					} else if (root.right != null && !root.right.equals("deleted")) {
						CloudNode rightNode = getNode(root.right);
						updateNode(root.nodeId, rightNode.data, rightNode.left, rightNode.right);
					} else {
						// make empty
					}
					
					
				} else if (root.data.compareTo(data) > 0) {
					System.out.println("DELETE: going left");
					String newLeftId = delete(data, getNode(root.left));
					// then change this node's left to newLeftId
					updateNode(root.nodeId, root.data, newLeftId, root.right);
					
				} else if (root.data.compareTo(data) < 0) {
					System.out.println("DELETE: going right");
					String newRightId = delete(data, getNode(root.right));
					// then change this node's left to newLeftId
					updateNode(root.nodeId, root.data, root.left, newRightId);
					
				}
//				root = delete(data, root);
				
				return true;
			} else {
				return false;
			}
		}
	}
	
	private String delete(String data, CloudNode node) {
		String nodeId = null;
		
		if (node == null) {
			return null;
		}
//		System.out.println("comparing " + node.data + " with " + data);
		if (node.data.equals(data)) {
//			System.out.println("Found " + node.data);
			
			nodeId = removeNode(node);
			
		} else if (node.data.compareTo(data) > 0) {
//			node.left = delete(data, getNode(node.left));
			
			String newLeftId = delete(data, getNode(node.left));
			// then change this node's left to newLeftId
			updateNode(node.nodeId, node.data, newLeftId, node.right);
			
		} else if (node.data.compareTo(data) < 0) {
//			node.right = delete(data, node.right);
			
			String newRightId = delete(data, getNode(node.right));
			// then change this node's right to newRightId
			updateNode(node.nodeId, node.data, node.left, newRightId);
		} 
		
		return nodeId;
	}

	private String removeNode(CloudNode node) {
		String nodeId = null;
		
		
		if (node.left != null && !node.left.equals("deleted") && node.right != null && !node.right.equals("deleted")) {
			CloudNode largest = largest(getNode(node.left));
			
			System.out.println("We're going to replace " + node.nodeId + "'s data (" + node.data + ") with " + largest.data);
			// update this node's data
			updateNode(node.nodeId, largest.data, node.left, node.right);
//			node.data = largest.data;
//			System.out.println("largest is " + largest.data);
			
			
//			node.left = delete(largest.data, node.left);
			String newLeftId = delete(largest.data, getNode(node.left));
			// then change this node's left to newLeftId
			updateNode(node.nodeId, largest.data, newLeftId, node.right);
			
		} else if ((node.left == null || node.left.equals("deleted")) && node.right != null && !node.right.equals("deleted")) {
			CloudNode rightNode = getNode(node.right);
			if (rightNode != null) {
				nodeId = rightNode.nodeId;
			} else {
				nodeId = "deleted";
			}
			
		} else if ((node.right == null || node.right.equals("deleted")) && node.left != null && !node.left.equals("deleted")) {
			CloudNode leftNode = getNode(node.left);
			if (leftNode != null) {
				nodeId = leftNode.nodeId;
			} else {
				nodeId = "deleted";
			}
			
			
		} else {
			nodeId = "deleted";
			
		}
		return nodeId;
	}

	private CloudNode largest(CloudNode node) {
		if (node == null) {
			return null;
		}
		if (node.right != null) {
			return largest(getNode(node.right));
		} else {
			return node;
		}
	}
	
	private boolean isLeafNode(CloudNode node) {
		return (node.left == null && node.right == null);
	}
	
	
	private class CloudNode implements Comparable {
		
		private String nodeId;
		private String data;
		private String left;
		private String right;
		
		public CloudNode(String nodeId, String data, String left, String right) {
			this.nodeId = nodeId;
			this.data = data;
			this.left = left;
			this.right = right;

		}
		public CloudNode(String nodeId, String data) {
			this(nodeId, data, null, null);
		}
		
		@Override
		public int compareTo(Object o) {
			
			String other = ((CloudNode) o).data;
			
			if (data.compareTo(other) < 0) {
				return  -1;
			} else if (data.compareTo(other) > 0) {
				return 1;
			} else {
				return 0;
			}
		}
		
		@Override
		public String toString() {
			return "CloudNode: nodeId: " + nodeId + ", data: " + data + ", left: " + left + ", right: " + right;
		}
	}
	
//	public int size() {
//	CloudNode root = getNode("root");
//	return root.size;
//}
//	private void incrementSize() {
//		CloudNode root = getNode("root");
//		int size = root.size;
//		
//		CloudNode newNode = new CloudNode("root", root.data, null, null, (size + 1));
//		cache.put("root", newNode);
//		dynamoDBAccess.updateItem(treeName, "root", root.data, root.left, root.right, "" + (size + 1)); // size++
//	}
//	
//	private void decrementSize() {
//		CloudNode root = getNode("root");
//		int size = root.size;
//		
//		CloudNode newNode = new CloudNode("root", root.data, null, null, (size - 1));
//		cache.put("root", newNode);
//		dynamoDBAccess.updateItem(treeName, "root", root.data, root.left, root.right, "" + (size - 1)); // size--
//	}
	
	
	
	public void inorderTraversal() {
		str = "CloudBST In-order traversal: {";
		CloudNode root = getNode("root");
		inorderTraversal(root);
		System.out.println(str + "}");
	}
	
	private void inorderTraversal(CloudNode node) {
		if (node == null) {
			return;
		} else {
			if (node.left != null) {
				CloudNode left = getNode(node.left);
				inorderTraversal(left);
			}
			str += node.data + ", ";
			
			if (node.right != null) {
				CloudNode right = getNode(node.right);
				inorderTraversal(right);
			}
			
		}
	}
	
}
