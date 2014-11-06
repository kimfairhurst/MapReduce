MapReduce
=========

A MapReduce model built on the framework of Adobe Spark (Python).

Built to provide a solution to a "sliding puzzle" using a breadth-search solver and strongly-solve algorithm.

Takes as input a height, width, and solution position represented as a Python tuple. Expands the solution with a breadth-first-search (BFS) tree and generates a mapping between the solution and every possible position. The final output is a string represented by (key, value) pairings of the position and their level (steps away from solution). 

For example, for a 2x2 puzzle the output would look as follows: 
0 ('A', 'B', 'C', '-')
1 ('A', '-', 'C', 'B')
1 ('A', 'B', '-', 'C')
2 ('-', 'A', 'C', 'B')
... (continues until all possible positions are reached)


For more details of project specs see: 
<br> http://inst.eecs.berkeley.edu/~cs61c/fa14/projs/02/
<br> http://inst.eecs.berkeley.edu/~cs61c/fa14/projs/02/index2.html
