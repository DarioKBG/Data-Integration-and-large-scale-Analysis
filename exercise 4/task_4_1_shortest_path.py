import os     # imports OS module to work with the files and directories directly thtough os
import xml.etree.ElementTree as ET     # imports ElementTree for parsing XML files
from collections import deque, defaultdict     # imports deque and defaultdict for saving the required stations files accordingly


dir = "timetables_compressed/250902_250909/2509021200"    # a directory out of timetables dataset is chosen

graph = defaultdict(set)   # create a graph in which the keys are stations and all the neighbors are values	

for file in os.listdir(dir):    # checks for and processes every xml file found in the directory
    if not file.endswith(".xml"):
        continue

    tree = ET.parse(os.path.join(dir, file))    # Parse the XML files and create an ElementTree object
    root = tree.getroot()    # this gets the root element of the XML tree

    for s in root.findall("s"):    # this accesses the arrival and departure element under every 's'
        ar = s.find("ar")
        dp = s.find("dp")
    
        for node in [ar, dp]:   # goes through the entire ar and dp elements to find the attribute "ppth"
            if node is not None and "ppth" in node.attrib:  
                stations = node.attrib["ppth"].split("|")    # creates a list of stations out of the paths

                for i in range(len(stations) - 1):   # this adds all the neighbors as values to the stations
                    a, b = stations[i], stations[i + 1]
                    graph[a].add(b)
                    graph[b].add(a)   # both directions are mentioned because the graph is undirected


def shortest_path(graph, source, target):
    visited = set([source])     # this is a set that keeps the track of all the visited stations, this helps us to avoid a loop, processing the stations already visited
    queue = deque([[source]])   # this is to tract the path till the code find the target. it tracts different one - the right and the worng ones

    while queue:  # a queue has multiple paths and this line starts searching them
        path = queue.popleft()     # a quene is taken
        node = path[-1]            # the last station of the path is taken

        if node == target:      # if the target is found, then the corresponding path is returned
            return path 

        for neighbor in graph[node]:     # explores all the values (neighbours) of this key (station)
            if neighbor not in visited:   # check whether the neighbor station is visited (processed)
                visited.add(neighbor)      # if not, the station is added into the visited list
                queue.append(path + [neighbor])    # add the station name to the path 

    return None


source = "Berlin Hbf"
target = "Berlin Ostkreuz"

path = shortest_path(graph, source, target)    # calls the shortest path function

if path:
    print("Shortest path:")
    print(" -> ".join(path))
    print("Number of transfers:", len(path) - 1)
else:
    print("No path found")
