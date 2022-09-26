
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeperation")
sc = SparkContext(conf=conf)


# The characters we wish to find the degree of freedom between:

startCharacterID = 5306 #SpiderMan
targetCharacterID = 14 #Adam


hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []

    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GREY'
        distance = 0

    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile("/home/administrator/PycharmProjects/sparkProject/spark_advanced/Data/Marvel+Graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == 'GREY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GREY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID,([], newDistance, newColor))
            results.append(newEntry)

        # We have processed this node, so color is black
        color = 'Black'

    # Emit the input node so we don't lose it.
    results.append((characterID, (connections,distance,color)))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    #See if one is the original node with its connections.
    # If so, preserve them.

    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance2):
        distance = distance1

    if (distance2 < distance1):
        distance = distance2


    #Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


# Main Program here:

iterationRdd = createStartingRdd()

for iteration in range(0,10):
    print("Running BFS iteration# " + str(iteration+1))

    mapped = iterationRdd.flatMap(bfsMap)

    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
              + " different direction(s).")
        break

    iterationRdd = mapped.reduceByKey(bfsReduce)

