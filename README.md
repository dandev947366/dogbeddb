# dogbeddb
 Dogbed DB with Python using Binary Search Tree 

## storage.py
The Storage class is designed to handle low-level file operations, such as reading, writing, and locking, with a focus on ensuring thread safety and persistent storage. 

## node.py
This file defines the core data structures for a persistent, disk-backed binary tree, where each node and its references can be serialized and stored to disk using a custom Storage class. 