README
###Task1###
Git: https://github.com/kadermak/DET1/tree/main

###File structure###
Project folder: \TopItem
Main class: \TopItem
Unit test:  \src\test\scala\ ManualJoinTest.scala
Input file: detections.parquet
Output file: outputFile.txt
Build file: build.sbt
Gitignore file: \.gitignore

###Setup###
1.	Install sbt , Dependency : Java version 8 -11sbt
2.	Go to cmd-> cd to project directory run-> sbt compile
3.	To style check run -> sbt “run scalastyle”
4.	To run unit test run -> sbt test
5.	To run main class -> sbt “run TopItemsRank”
6.	To output to txt file run -> sbt “run TopItemsRank” > outputFile.txt

Sample output:
[info] running TopItemsRank TopItemsRank
PROCESSING TOP ITEM###
LocationID: 4,  Item_Rank: 1 , Item_Name: car, Item_Count: 2,
LocationID: 3,  Item_Rank: 2 , Item_Name: cat, Item_Count: 1,
LocationID: 9,  Item_Rank: 3 , Item_Name: person, Item_Count: 1,
COMPLETED###
[success] Total time: 40 s, completed  

 
###Code Design###
1.	To Change input file path or TopX filter , edit line 11 -14 section
2.	Start spark session
3.	Load the parquet file to rdd
4.	Drop duplicates based on detection_oid and store to distinctDetectionsRdd
5.	To avoid data skew add salting method,  to the key to spread out the data more evenly 
6.	Join the detections and locations RDDs on geographical_location_oid , used manual join 
7.	Group by location and count items
8.	Assign item rank on location, count
9.	Filter outputRdd on topX item

