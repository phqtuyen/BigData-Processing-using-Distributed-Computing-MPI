###################################################################################
## This program takes two json files as input where :
##		The first file describes the grid of a portion of Australian map
##		The second file describes twitter meta information 
##	The program then count the number of tweets in each grid cell, cols and rows
## it then output the corresponding results and rank them in the descending order.
## Th author has made use of parallel computing paradigm, specifically MPI 
## which stands for Message Passing Interface
## further description is provided in the attached written report
##
## Author : Tuyen Quang Pham
## ID : 381450 


from mpi4py import MPI
import json
import time

start = time.time()
#change fileNAme1 to targetd grid file
fileName1 = "melbGrid.json"
#change fileName2 to the targeted twitter file
fileName2 = "bigTwitter.json"

# Class which contains information for each grid cell
class CellObj:

	def __init__(self, id, xs, ys):
		self.id = id
		self.xs = xs
		self.ys = ys
		self.count = 0

	def isInCell(self,x,y):
		return (x >= self.xs[0] and x <= self.xs[1] and y >= self.ys[0] and y<= self.ys[1])

	def incCount(self):
		self.count+=1			



# Class which contains information for each grid col or row
class ColRowObj:

	def __init__(self, id):
		self.id = id
		self.count = 0

	def incCount(self):
		self.count+=1


comm = MPI.COMM_WORLD
##############################################
#process melbGrid file
if comm.rank == 0 : #master process
	cellList = []
	rowSet = set([])
	colSet = set([])	
	f = open(fileName1,'r')
	gridPos = json.loads(f.read())
	#create list of cell object
	for cell in gridPos["features"] :
		cellObj = CellObj(cell["properties"]["id"],
							[cell["properties"]["xmin"],cell["properties"]["xmax"]],
							[cell["properties"]["ymin"],cell["properties"]["ymax"]])
		cellList.append(cellObj)
		rowSet.add(cell["properties"]["id"][0]) # store columns and rows
		colSet.add(cell["properties"]["id"][1])

	f.close()
	for i in range(1, comm.size) : # send the grid information to other processes
		comm.send(cellList, dest = i)

else : # slave process receive grid info from master
	cellList = comm.recv(source = 0)

comm.Barrier()
####################################################
#process twitter file
coorList = []
if comm.rank == 0 : #master read in twitter and distribute to slaves according to a cylic cyle
	destt = -1		# i.e start with 0 then 1, 2, 3 ,.. 8, then back to 0,.. (assume there are 8 processes)
	f = open(fileName2,'r')
	f.readline();
	for line in f :
		tweet = line.strip().strip(',')
		if (len(tweet) > 1) :
			destt = (destt + 1) % comm.size
			if (destt == 0) :
				tweetdata = json.loads(tweet)
				coord = tweetdata["json"]["coordinates"]["coordinates"]
				coorList.append(coord)
			else:	
				comm.send(tweet, dest=destt)
	## signal the end of input file			
	end = "1"	
	f.close()
	for i in range(1,comm.size) :
		comm.send(end, dest=i)

else : #slave receive twitter and extract coordinates
	end = 0
	while (end != 1) :
		data = comm.recv(source=0)
		if (data == "1") :
			end = 1
		else :
			tweetdata = json.loads(data)
			coord = tweetdata["json"]["coordinates"]["coordinates"]
			coorList.append(coord)

##################################################
## each process count tweets based on their coordinates
for coord in coorList :
	for cell in cellList :
		if cell.isInCell(coord[0], coord[1]) :
			cell.incCount()
			break;


comm.Barrier()

################################################
## after the counting process complete
## slaves report back to master and add up the total

if comm.rank == 0 : 
	for i in range(1, comm.size) :
		data = comm.recv(source =i)
		for j in range(0,len(data)):
			cellList[j].count += data[j].count
else : 
	comm.send(cellList, dest = 0)


comm.Barrier()
##################################################
## sort results and output
if comm.rank == 0 :
	cellList.sort(key = lambda x:x.count, reverse=True)

	rowList = []
	for row in rowSet:
		rowList.append(ColRowObj(row))
	colList = []	
	for col in colSet:
		colList.append(ColRowObj(col))
	for row in rowList :
		for cell in cellList:
			if row.id in cell.id :
				row.count += cell.count
	
	for col in (colList):
		for cell in cellList:
			if col.id in cell.id :
				col.count += cell.count			 

	rowList.sort(key = lambda x:x.count, reverse=True)
	colList.sort(key = lambda x:x.count, reverse=True)				

	print ("Order (rank) the Grid boxes based on the total number of tweets made in each box and return the total count of tweets in each box")
	for cell in cellList :
		print (cell.id + ": " + str(cell.count) + " tweets,\n")
	
	print ("Order (rank) the rows based on the total number of tweets in each row")
	for row in rowList:
		print (row.id + "-Row: " + str(row.count) + " tweets,\n")

	print ("Order (rank) the column based on the total number of tweets in each column")
	for col in colList:
		print ("Column " + col.id + ": " + str(col.count) + " tweets,\n")

	end = time.time()	
	#print("Time Elapsed + " + str(end - start))


	









