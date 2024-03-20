## Project 4 Report


### 1. Basic information
Team #: 1
Github Repo Link: self
Student 1 UCI NetID: nghatysa
Student 1 Name: Neha Ghaty Satish
Student 2 UCI NetID (if applicable): ssannell
Student 2 Name (if applicable): Sanket Hanumappa Sannellappar


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).
- A table named "IX_Table" holds all indexes of all the tables.
- Each index is identified by the table_ID of the table it is in, index name, datatype and index filename. 


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)
- It calls getNextTuple of the iterator passed to it.
- Applies the filter condition on the returned tuple and returns it if it satisfies the filter condition. 


### 4. Project
- Describe how your project works.
- It calls getNextTuple of the iterator passed to it.
- Projects the selected attributes values in all the tuples and returned only those. 


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)
- We load a block of numPages from the left table into memory and load the tuples in an unordered_map.
- Then we load each page of the right table and go through the records in that page to see if there are any matching tuples in the hash table.
- If there are any matches, the tuples are joined and returned to the caller. 


### 6. Index Nested Loop Join
- Describe how your index nested loop join works.



### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.
- Whenever getNextEntry is called for Aggregation, it scans the whole table and updates the aggregation operators by considering the attribute's value in the current tuple.
- The basic aggregations are max, min, sum, count and average. 

- Describe how your group-based aggregation works. (If you have implemented this feature)



### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.



- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.
- Neha: Block nested loop join, Filter, Relation Manager extensions
- Sanket: Project, Aggregate, Relation manager extensions 


### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)

### 12. Texera Video (Extra Credit)
https://drive.google.com/file/d/1IfBqli0w3bBJ7ORL-H_Nbu51wyma6Qde/view
