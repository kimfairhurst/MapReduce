from pyspark import SparkContext
import Sliding, argparse

""" Returns the minimum of the two values, AKA the (key, value) pair with the lower level """
def bfs_reduce(value1, value2):
    return min(value1, value2)

""" Returns a list of the parentValue tuple and its child tuples"""
def bfs_flat_map(parentValue):
    mappedList = []
    mappedList.append(parentValue)

    # if the parentValue is from the last level we checked, look for its children and append them 
    if parentValue[1] == level - 1:
        children = Sliding.children(WIDTH, HEIGHT, parentValue[0])
        for element in children:
            mappedList.append((element, level))

    return mappedList

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. 
    sc = SparkContext(master, "python")


    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT= height
    WIDTH= width
    level = 0 

    sol = Sliding.solution(WIDTH, HEIGHT)

    # Create a list of just the solution
    solList = []
    solList.append((sol, 0))
    levelList = sc.parallelize(solList)
    counter = 0

    # Continue until all positions have been found.
    while level != -1:
        level += 1
        counter += 1
        levelList = levelList.flatMap(bfs_flat_map) \
                             .reduceByKey(bfs_reduce)

        # Checks if any positions were added
        newList = levelList.filter(lambda x: x[1] == level)
        if newList.count() == 0:
            level = -1

        # Repartitions every 32 steps
        if counter % 32 == 0:
            levelList = levelList.partitionBy(16)

    arr = levelList.collect()

    for elem in arr:
        finalStr = str(elem[1]) + " " + str(elem[0])
        output(finalStr)

    sc.stop()



def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
