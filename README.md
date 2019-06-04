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
* Initially, take the first element from each run and add it to the heap. These elements are the min elements for each run.
   - Loop while the least element (top of the heap) is **INT_MAX**.
     * Pick the top node from the min heap.
     * Write the element to the *outputFile*.
     * Get the *runID* of the popped element. The next element will be picked from the run corresponding to this *runID*.
     * Read the next item from the run . If it's **EOF**, mark the  item as **INT_MAX**.
     * Put **INT_MAX** to the top of the heap and heapify.
* At the end of the Merge Phase **outputFile** will have all the elements in sorted order .

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
<b> We have see EOF. So mark the read element as <i>INT_MAX </i></b>. </br>

       INT_MAX                                 7
        /  \                                 /  \
       7    9      Heapify -->              8     9
      /  \                                 / \
    10   8                               10   INT_MAX

Pick the least element *6* and move it to outputFile - *1 2 3 4 5 6 7*. </br>
If we loop this process, we would reach a point where the heap will look like below
and the </br> outputFile - *1 2 3 4 5 6 7 8 9 10*. </br>We would also break at the point when the min element from heap becomes *INT_MAX*.

                           INT_MAX
                            /   \
                        INT_MAX  INT_MAX
                        /    \
                     INT_MAX INT_MAX

**Benchmarks**

**Name** &nbsp;&nbsp; **Runs** &nbsp;&nbsp; **Time Taken** &nbsp;&nbsp; **Memory Consumed** &nbsp;&nbsp; **Allocations** </br>

*(For 1L entries)* </br>
BenchmarkExtMergeSort-4 &nbsp;&nbsp; *20* &nbsp;&nbsp;	*88639812* ns/op	&nbsp;&nbsp; *16550994* B/op &nbsp;&nbsp; *700387* allocs/op </br>
