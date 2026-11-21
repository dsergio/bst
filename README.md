# CloudBST

A cloud-backed **Binary Search Tree (BST)** implemented in Java with **Amazon DynamoDB** as its persistent storage layer.

CloudBST stores each tree node as a DynamoDB item and connects nodes through `left` and `right` child identifiers. A lightweight in-memory cache reduces repeat reads during tree operations, while the tree API provides insertion, lookup, deletion, and in-order traversal.

## Highlights

- **DynamoDB persistence** — node data and child references are stored in a DynamoDB table.
- **Binary search tree ordering** — values are compared lexicographically using Java's `String.compareTo`.
- **On-demand node loading** — nodes are fetched from DynamoDB when they are not already cached.
- **In-memory cache** — retrieved and updated nodes are kept in a `HashMap`.
- **Core tree operations** — insert values, test membership, delete values, and print an in-order traversal.
- **Simple test harness** — `Tester.java` includes a sample animal-name dataset.

## How it is structured

| File | Responsibility |
| --- | --- |
| `CloudBST.java` | Tree implementation, node cache, traversal, and tree operations |
| `BSTDynamoDBAccess.java` | AWS DynamoDB client setup and item/table operations |
| `Tester.java` | Example entry point and sample data |

### Storage model

Each tree uses its own DynamoDB table. The table is created with a string partition key named `nodeId`.

| Attribute | Meaning |
| --- | --- |
| `nodeId` | Unique identifier for a node; the table's partition key |
| `data` | String value held by the node |
| `left` | Identifier of the left child, when present |
| `right` | Identifier of the right child, when present |

The root node is stored with the fixed identifier `root`. Newly inserted child nodes receive UUID-based identifiers. The implementation uses the string value `deleted` as a child-reference sentinel during deletion.

## Requirements

- Java (a JDK compatible with the project's AWS SDK dependency)
- An AWS account with access to Amazon DynamoDB
- AWS credentials configured locally
- AWS SDK for Java **1.x** DynamoDB libraries


## AWS configuration

`BSTDynamoDBAccess` loads credentials using `ProfileCredentialsProvider`, which reads the standard AWS shared credentials file.

1. Configure a profile in `~/.aws/credentials` (or the equivalent location for your operating system).
2. Ensure that the profile has permission to create, describe, scan, put, and update DynamoDB tables/items.
3. The client is configured to use the AWS region **`us-west-2`**.

Example credentials-file shape (use your own credentials; never commit them):

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Keep credentials outside the repository. Do not put access keys in source code or commit them to version control.

## Getting started

After adding the AWS SDK for Java 1.x DynamoDB dependency and configuring AWS credentials, compile the three source files and run the test harness.

For example, if the SDK JARs are available in a local `lib/` directory:

```bash
javac -cp "lib/*" BSTDynamoDBAccess.java CloudBST.java Tester.java
java -cp ".:lib/*" Tester
```

On Windows, use `;` instead of `:` in the Java classpath:

```bat
java -cp ".;lib/*" Tester
```

The harness constructs a tree named `animal_tree2`:

```java
CloudBST cloudBST = new CloudBST("animal_tree2");
```

Constructing a `CloudBST` creates the DynamoDB table if it does not already exist, then waits for it to become active.

## Using the tree

Create a tree and insert values:

```java
CloudBST tree = new CloudBST("my_tree");

tree.insert("Mango");
tree.insert("Apple");
tree.insert("Pear");

System.out.println(tree.contains("Apple")); // true
System.out.println(tree.contains("Banana")); // false

tree.inorderTraversal();
```

An in-order traversal visits the left subtree, the current node, and then the right subtree. For this BST, the resulting values are in lexicographical order.

### Available operations

| Method | Description |
| --- | --- |
| `insert(String data)` | Inserts a value according to BST ordering |
| `contains(String data)` | Returns whether the value is present |
| `delete(String data)` | Attempts to delete a value |
| `inorderTraversal()` | Prints the values in in-order sequence |
| `printCache()` | Prints the nodes currently held in the local cache |

The DynamoDB access class also provides lower-level methods for creating tables, adding items, retrieving nodes, and updating node attributes.

## Notes and current limitations

- **String-only values:** comparisons use `String.compareTo`, so ordering is lexicographical rather than numeric or locale-aware.
- **AWS costs and permissions:** table creation and read/write operations use real AWS resources and may incur charges. The table is configured with provisioned read and write capacity of 1 unit each.
- **Cache scope:** the cache belongs to a `CloudBST` instance. Changes made by another instance or external DynamoDB client are not automatically reflected in an existing cache.
- **Deletion behavior:** deletion is implemented using child-reference updates and the `deleted` sentinel. Review and test edge cases (especially deleting the root and nodes with two children) before relying on it for production data.
- **Test harness behavior:** the sample animal list is present in `Tester.java`, but the `insert` call inside its random-selection loop is commented out in the supplied source. As supplied, the harness does not populate the tree with those random animals.

## Future improvements

- Add a Maven or Gradle build with a pinned AWS SDK dependency.
- Replace scan-based node lookup with a key-based DynamoDB `GetItem` operation.
- Add automated tests for insertion, duplicate values, lookup, and all deletion cases.
- Improve cache invalidation and consistency when multiple clients access the same tree.
- Add support for typed values, balancing, and tree-size tracking.

