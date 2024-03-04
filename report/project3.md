## Project 3 Report


### 1. Basic information
 - Team #: 1
 - Github Repo Link: self 
 - Student 1 UCI NetID: nghatysa
 - Student 1 Name: Neha Ghaty Satish
 - Student 2 UCI NetID (if applicable): ssannell
 - Student 2 Name (if applicable): Sanket Hanumappa Sannellappar


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 
Meta data page has a pointer to the root of the tree. The pointer is page number. 


### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:
  1) Keys are stored which were received by either pushing up or copying up
  2) There are num(keys) + 1 children stored in between pages which point to the nodes between any two keys. 
  
  - entries on leaf nodes:
  1) Keys
  2) Each key has a corresponding RID. 


### 4. Page Format
- Show your internal-page (non-leaf node) design.
1) Metadata at the end of the page consists of the following:
    a) Free space
    b) Number of entries
    c) Leaf indicator
2) Entries are inserting in the beginning of the page


- Show your leaf-page (leaf node) design.
1) Metadata at the end of the page consists of the following:
    a) Free space
    b) Number of entries
    c) Leaf indicator
    d) Page number of the leaf node
    e) Next pointer pointing to the next leaf node
    f) Previous pointer pointing to the previous leaf node
2) Entries are inserting in the beginning of the page


### 5. Describe the following operation logic.
- Split
There are two broad categories of splits:
1) Internal node:
   a) Non-root:
      - a new internal node is created
      - half the number of keys and children are transfered to it.
      - The first key of the new internal node is pushed up by returning its page number to the caller and erasing that node from its    
        current position.
      - Caller recieves the new node's page number and inserts it. It also adds the pointer.
   b) Root:
      - Same as above, except that a new root is created and the keys and children are added to it.
    
2) Leaf Node:
   a) Non-root:
      - A new leaf node is created
      - half the number of keys and RIDs are transfered to it.
      - The first key of the new leafnode is copied up by returning its page number to the caller.
      - Set the next and previous pointers 


- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)
1. We find the node to delete by comparing its key and RID with the keys in the internal node and navigating through the pointers. 
2. We delete the key from there.  


- Duplicate key span in a page



- Duplicate key span multiple pages (if applicable)



### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

We have not added any .cc or .h file. 

- Other implementation details:



### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.

Sanket: insertEntry, deleteEntry, scan, getNextEntry
Neha: insertEntry, scan, printBTree, create/destroy/open/close file

### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
