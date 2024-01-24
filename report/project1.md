## Project 1 Report


### 1. Basic information
 - Team #: 1
 - Github Repo Link: https://github.com/nehaghaty/cs222-winter24-nehaghaty/
 - Student 1 UCI NetID: 91008980
 - Student 1 Name: Neha Ghaty Satish
 - Student 2 UCI NetID (if applicable): 38926436
 - Student 2 Name (if applicable): Sanket Hanumappa Sannellappar


### 2. Internal Record Format
- Record Format:

![recordformat drawio](https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/50c3f87a-12bd-48c0-8ee8-e3431186f2e9)


- Describe how you store a null field.

![nullformat drawio](https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/1434a976-0f18-46b5-a52a-1667e54052e4)

A. The offset of a null field points to the end of he previous record and the corresponding bit in the null bit array is set to 1.

- Describe how you store a VarChar field.

1. If the size of the VarChar field exceeds the specified maximum value, we store max bytes of the field.
2. Otherwise we store the number of bytes provided in the input data.

- Describe how your record design satisfies O(1) field access.
 1. We know the offset of the starting field as this is a constant value (bytes for num fields + bytes for null bit array + bytes for offsets).
 2. We store the end of each field value in the offset fields.
 3. Using a combination of these, we can reach all field in constant time.

### 3. Page Format
- Show your page format design.



- Explain your slot directory design if applicable.
   Slot directory consists of the following from right to left:
  1. Number of free bytes on the page: 4 bytes
  2. Number of records on page: 4 bytes
  3. Records pointers for each record containing offset of record and record length   


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

```
 Iterate through Existing Pages
  Check if a Page has Sufficient Space i.e > (recordSize+slotSize)
  Write record to Existing Page Content if Found
 Create a New Page if No Existing Page is Found
```
   
            

- How many hidden pages are utilized in your design?
  A. 1 



- Show your hidden page(s) format design if applicable
  A. It just stores the page counters: appendPageCounter, readPageCounter and writePageCounter sequentially.



### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.

PFM: 
Sanket:  writePage, appendPage, collectCounterValues
Neha: createFile, destroyFile, openFile, closeFile

Shared: readPage

RBFM: 

Sanket: Record serialization and deserialization, printRecord, readRecord
Neha: Selecting page for inserting record, creating and updating page directory, code modularization

Report: shared by both
Debugging: shared by both

### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)

- One test, insert_and_read_massive_records, passes on our local Ubuntu 22.04 machines but still fails on the autograder for some reason. 

```
[       OK ] RBFM_Test.insert_and_read_massive_records (737 ms)
[----------] 1 test from RBFM_Test (737 ms total)
[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (737 ms total)
[  PASSED  ] 1 test.
1/1 Test #18: RBFM_Test.insert_and_read_massive_records ...   Passed    0.75 sec
The following tests passed:
	RBFM_Test.insert_and_read_massive_records
100% tests passed, 0 tests failed out of 1
Total Test time (real) =   0.78 sec

  ```


- Feedback on the project to help improve the project. (optional)
