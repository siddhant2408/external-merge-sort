# External Merge Sort

[External Sort](https://en.wikipedia.org/wiki/External_sorting) for large files that cannot be loaded in memory using merge sort approach.

**Problem Stement**

All sorting algorithms work within RAM. When the data to be sorted does not fit into the RAM and instead resides in the slower external memory(usually a hard drive), this
technique is used.

**Solution**

* Split the file into multiple smaller files called runs.
* Sort the runs using some efficient sorting algorithm in-memory, like Quick Sort.
* Do a [K-way merge](https://en.wikipedia.org/wiki/K-way_merge_algorithm) with all the runs into an output file.

**Algorithm**

> Split Phase
* Get a batch of *runSize* elements from the *inputFile*.
* Sort the elements using *Quick Sort*
* Write the sorted elements to a run file.
* The sorting and writing to the run file is done in parallel. Each unsorted list is passed to a new goroutine which quick sorts and writes it to the run file.
* Repeat the above steps till *EOF* reached.

> Merge Phase
* For that, we will use a min heap. Each heapnode will store the actual entry read from the run and also the *runID* which owns it.

* Initially, take the first element from each run and check if it's already present in the heap. If present, merge it with the heap element, else add it to the heap. With this approach, we can ensure that there are no duplicates(email/sms) in the heap.

* We also keep a map of all the heap elements i.e *heapMap*.

* Loop while the least element (top of the heap) is **MAX_ELE**.
  * Get the *runID* of the top node from min heap.
  * Get the *nextElement* from the run file with *runID* fetched above.
    * If we encounter **EOF**, pop the min element from the heap and add **MAX_ELE** to the heap.
    * Else, check if *nextElement* is already present in the *heapMap*.
      * If yes, then merge (*last one wins*) the *nextElement* with the element in the heap.
      * Else, pop the min element from the heap, push *nextElement* to the heap and update the *heapMap*. 

## Example

Say We have a file with the following contents:

5 8 6 3 7 1 4 9 10 2

In Split Phase, we split them into sorted runs in 5 separate temp files.

temp1 - 5 ,8   &nbsp;&nbsp; temp2 - 3 ,6      &nbsp;&nbsp;  temp3 - 1, 7 &nbsp;&nbsp;  temp4 -4 , 9  &nbsp;&nbsp; temp5 - 2 ,10 

Next construct a Min Heap with top element from each files.

                             1
                           /  \
                          2    5
                        /  \
                       4    3

Now pick the least element from the min heap and write it to outputFile - *1*. </br>
Find the next element from the file which owns min element *1*. </br>
The no is *7* from temp3. Move it to heap.

          7                                    2
        /  \                                 /  \
       2     5      Heapify -->             3    5
      /  \                                 / \
     4    3                               4   7

Pick the least element *2* and move it to outputFile - *1 2*. </br>
Finds the next element of the file which owns min element *2*. </br>
The no is *10* from temp5. Move it to heap.

          10                                   3
        /  \                                 /  \
       3     5      Heapify -->             4    5
      /  \                                 / \
     4    7                               10   7

Pick the least element *3* and move it to outputFile - *1 2 3*. </br>
Find the next element from the file which owns min element *3*. </br>
The no is *6* from temp2. Move it to heap.

          6                                   4
        /  \                                 /  \
       4     5      Heapify -->             6    5
      /  \                                 / \
    10   7                               10   7

Pick the least element *4* and move it to outputFile - *1 2 3 4*. </br>
Find the next element of the file which owns min element *4*. </br>
The no is *9* from temp4. Move it to heap.

          9                                   5
        /  \                                 /  \
       6     5      Heapify -->             6    9
      /  \                                 / \
    10   7                               10   7

Pick the least element *5* and move it to outputFile - *1 2 3 4 5*. </br>
Find the next element of the file which owns min element *5*. </br>
The no is *8* from temp1. Move it to heap.

          8                                   6
        /  \                                 /  \
       6     9      Heapify -->             7    9
      /  \                                 / \
    10   7                               10   8

Pick the least element *6* and move it to outputFile - *1 2 3 4 5 6*. </br>
Find the next element of the file which owns min element *5*. </br>
<b> We have see EOF. So mark the read element as <i>MAX_ELE </i></b>. </br>

       MAX_ELE                                 7
        /  \                                 /  \
       7    9      Heapify -->              8     9
      /  \                                 / \
    10   8                               10   MAX_ELE

Pick the least element *6* and move it to outputFile - *1 2 3 4 5 6 7*. </br>
If we loop this process, we would reach a point where the heap will look like below
and the </br> outputFile - *1 2 3 4 5 6 7 8 9 10*. </br>We would also break at the point when the min element from heap becomes *MAX_ELE*.

                           MAX_ELE
                            /   \
                        MAX_ELE  MAX_ELE
                        /    \
                     MAX_ELE MAX_ELE

**Benchmarks**

| **Input Size**  | **Runs** | **Time Taken** | **Memory Consumed** | **Allocations** |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| 1,00,000  | *3* | *394* ms/op | *42* MB/op | *500225* allocs/op|
