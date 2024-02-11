## Project 2 Report


### 1. Basic information
 - Team #: 1
 - Github Repo Link: https://github.com/nehaghaty/cs222-winter24-nehaghaty/
 - Student 1 UCI NetID: nghatysa
 - Student 1 Name: Neha Ghaty Satish
 - Student 2 UCI NetID (if applicable): ssannell
 - Student 2 Name (if applicable): Sanket Hanumappa Sannellappar

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

  Tables table:
  
<img width="726" alt="Screenshot 2024-02-10 at 9 29 46 PM" src="https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/f1a530ae-3448-4f13-b713-e845b18898f9">

Columns table:

<img width="718" alt="Screenshot 2024-02-10 at 9 30 56 PM" src="https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/a8a655ec-723d-489a-b286-44600ba89960">


### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

<img width="720" alt="Screenshot 2024-02-10 at 9 33 35 PM" src="https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/3ea451a7-fd59-4deb-b78e-5acf6fab9ca2">


- Describe how you store a null field.

   - Same as previous design.

- Describe how you store a VarChar field.

   - Same as previous design.

- Describe how your record design satisfies O(1) field access.

   - Same as previous design.

### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.
   - Same as previous design.


- Explain your slot directory design if applicable.
    - Same as previous design



### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?
   - 1



- Show your hidden page(s) format design if applicable
   - Same as previous design



### 6. Describe the following operation logic.
- Delete a record
    - Logic:
        - Load the page containing the record specified by RID.
        - Check if the record is a tombstone (indicating the actual data is stored elsewhere).
            - If yes, delete the actual data using the redirected RID and then delete the tombstone record itself.
            - If no, proceed to delete the record directly from the current page.
        - For deletion, calculate the record's offset and length, and then shift subsequent data in the page to fill the gap left by the deleted record.
        - Update the slot directory to mark the record's slot as deleted and adjust offsets for subsequent records.
        - Update the page's metadata to reflect the new amount of free space.
        - Write the modified page back to the file.
        - Release any dynamically allocated memory. 



- Update a record
   - Logic: 
      - Allocate memory to read the target page using the given RID.
      - Read the page. If reading fails, return an error code.
      - Extract the old record's offset and length from the page.
      - Check if the old record is a tombstone (indicating the actual data is elsewhere):
          - If yes, follow the tombstone's redirect to the actual data's location and update oldRecordOffset and oldRecordLength.
      - Calculate the free space on the page and the number of slots.
      - Determine the size of the new record and build it in memory.
      - Compare the size of the new record to the old record:
          - If smaller, shift subsequent data up to reclaim space and update the page.
          - If the same size, simply overwrite the old record with the new one.
          - If larger but fits within the page's free space, shift subsequent data down to make room and update the page.
          - If larger and does not fit within the free space, allocate a new location for the record:
              - If the old record was a tombstone, update its redirect.
              - If not, create a new tombstone record pointing to the new location.
      - Write the updated page data back to the file.
      - Free all dynamically allocated memory.



- Scan on normal records



- Scan on deleted records



- Scan on updated records



### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- 



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
