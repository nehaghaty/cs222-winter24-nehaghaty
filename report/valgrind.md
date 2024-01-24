## Debugger and Valgrind Report

### 1. Basic information
 - Team #:
 - Github Repo Link:
 - Student 1 UCI NetID:
 - Student 1 Name:
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Using a Debugger
- Describe how you use a debugger (gdb, or lldb, or CLion debugger) to debug your code and show screenshots. 
For example, using breakpoints, step in/step out/step over, evaluate expressions, etc.

1. Set flags:
   ```
   cmake -DCMAKE_CXX_FLAGS=-g ../
   ```
2. Build code using cmake
3. Run debugger command for sample test:
   ```
   $gdb --args /home/nghatysa/cs222-winter24-nehaghaty/cmake-build-debug/rbfmtest_public "--gtest_filter=RBFM_Test_2.varchar_compact_size"
   ```
4. Set break point using `break filename:linenumber` and call `run`:
   
   <img width="1020" alt="Screenshot 2024-01-23 at 11 17 18 PM" src="https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/720846c7-c454-481e-ac11-729f624d19eb">

5. Hit breakpoint
   <img width="1394" alt="Screenshot 2024-01-23 at 11 17 23 PM" src="https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/cb05dd0d-c0cc-417e-932c-ea6bfa407733">



### 3. Using Valgrind
- Describe how you use Valgrind to detect memory leaks and other problems in your code and show screenshot of the Valgrind report.

- Command used:
  ```
  valgrind --leak-check=full \
      --show-leak-kinds=all \
      --track-origins=yes \
      --verbose \
      --log-file=valgrind-out.txt \
      ./pfmtest_public --gtest_filter=*
  ```

  Sample run:
  - <img width="1173" alt="Screenshot 2024-01-23 at 11 27 13 PM" src="https://github.com/nehaghaty/cs222-winter24-nehaghaty/assets/25128989/ecb75785-c96b-4e53-8e58-56b0eb777c76">

Link to PFM valgrind:
