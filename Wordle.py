# --------------------------
# shp_wodle.py
# a program for creating a wordle within the boundaries of a shapefile
# INPUT: a shapefile
# INPUT: set of word weights for each area in the shapefile
# OUTPUT: a jpg of the wordle
# --------------------------


# shapefile stuff
import shpUtils # for shapefile reading, requires dbfUtils.py
'''import matplotlib.pyplot as plt #for matplotlib'''

# wordle stuff
import cairo # for creating a raster surface
import numpy as np #for handling multi-dimensional arrays
'''import pyglet, pyglet.gl as gl #openGL wrapper for Python'''
import psycopg2
import time
from pprint import pprint as pp


import logging #intelligent logging....
logging.basicConfig(level=logging.INFO) # optional argument filename='example.log'

class TopicWords (object):
	''' Class TopicWords:
		Desc: connects to postgres topic output data table and generates input data for wordle
	'''
	def __init__(self, tableName):		
		# pre processing sql. 
		# create a new table from the union of two previous tables, 
		# order by nieghbourhood and then topic number
		'''create table geolda_van_merge as
		(select * from geolda_geolda_van_24 
		union select * from geolda_geolda_van_410
		order by name, topicnum)'''



		#Connect to postgres
		connString = "dbname='db_name' user='user' host='host' port='xx' password='password'"
		# select only three nHoods to keep problem size reasonable for now....
		selectString = "SELECT * FROM "+tableName
		logging.info("connecting to database")		
		conn = psycopg2.connect(connString) #connect to DB
		logging.info("creating cursor")
		#cursor for reading the nHoods from the tor_nhoods table
		nHoodCurr = conn.cursor()
		#get data from postgres table
		logging.info("executing select statement")
		nHoodCurr.execute(selectString)
		self.words=[]
		for topic in nHoodCurr:
			logging.info("accessing:"+topic[0])
			self.words.append((topic[3],'blank',int(float(topic[2])*100000),topic[0],topic[1])) #(word,link,weighted weight,parent polygon,topic family number:1-5)
			self.words.append((topic[5],'blank',int(float(topic[4])*100000),topic[0],topic[1]))
			self.words.append((topic[7],'blank',int(float(topic[6])*100000),topic[0],topic[1]))
			self.words.append((topic[9],'blank',int(float(topic[8])*100000),topic[0],topic[1]))
			self.words.append((topic[11],'blank',int(float(topic[10])*100000),topic[0],topic[1]))			
		logging.info("words loaded")
		#pp(self.words)


class Poly(object):
	''' Class Poly
	Data object that creates a polygon as a 2D numpy array
	'''
	def __init__(self, x,y,name): #need to implement the polygon name attribute.
		''' constructor
		Desc: the method takes the two lists and creates a 2xn dimensional array
		i.e. [ [2,4]
			   [5,6]]
		Param: x - a list of x values for a polygon
		Param: y - a list of y values for the same polygon

		Public variable: coords - the polygon coordinates
		Public Varibale: name   - the name of the polygon from the attribute table
		'''
		self.x = x #x coords as list
		self.y = y #y coords as list
		self.name = name # name of the polygon from shapefile
		self.coords = np.array([]).reshape(0,2)
		
		#read coordinates into numpy array
		for i in range(0,len(self.x)):
			self.coords = np.vstack([self.coords,[self.x[i],self.y[i]]])

		# logging.info(self.coords)

	def poly_shift(self, xSize, ySize):
		''' Method: poly_shift
			Desc:	returns an array of the polygon, but with the coordinates 
					translated into the space of the output surface
			Params:	xSize - the coord range to translate to, from 0-xSize
					ySize - the coord range to translate to, from 0-ySize
			Return:	a 2D numpy array of adjusted coordinates
		'''
		#strategy. 
		# 1. apply shift for polygon using absolute value of min, regardless of positive of negative
		# 2. apply shift back to origin reagrless of weather it is there or not
		# 3. stretch over new size

		#1. shift polygon over 
		self.coords[...,0]+=abs(self.coords[...,0].min())
		self.coords[...,1]+=abs(self.coords[...,1].min())

		#2. shift polygon back to origin
		self.coords[...,0]-=self.coords[...,0].min()
		self.coords[...,1]-=self.coords[...,1].min()

		#3. stretch over new range of values
		self.coords[...,0]=(self.coords[...,0].astype(float)/self.coords[...,0].ptp())*xSize
		self.coords[...,1]=(self.coords[...,1].astype(float)/self.coords[...,1].ptp())*ySize

		#4. return augmented array
		return self.coords 

class Shapefile(object):
	'''Class Shapefile
	Data Object to capture records from the shpUtils package
	'''
	def __init__(self,filename):
		self.filename = filename # the filename and location of the shapefile
		self.records = [] #list of easch poly in the shapefile
		self.minX = 9999
		self.maxX =-9999
		self.minY= 9999
		self.maxY= -9999

		shpRecords = shpUtils.loadShapefile(self.filename) 

		for i in range(0,len(shpRecords)):
			x=[]
			y=[]
			for j in range(0,len(shpRecords[i]['shp_data']['parts'][0]['points'])):
				tempx = float(shpRecords[i]['shp_data']['parts'][0]['points'][j]['x'])
				tempy = float(shpRecords[i]['shp_data']['parts'][0]['points'][j]['y'])
				x.append(tempx)
				y.append(tempy)

			name = shpRecords[i]['dbf_data']['NAME']
			#logging.info("reading name:"+name)
			#name = 'test'
			self.records.append(Poly(x,y,name))

		# Calculates the spatial extents. ideally this information is calculated in the above for loop, but i'm lazy and this is fast.
		for p in self.records:										# for each poly
			tX = min(p.coords[...,0])									# find the min value of X
			tY = min(p.coords[...,1])									# find the min value of Y
			if tX<self.minX: self.minX=tX 								# if the current poly's min x is smaller than recorded minX, set min to current
			if tY<self.minY: self.minY=tY 								# if the current poly's min y is smaller than recorded miny, set min to current
			tX = max(p.coords[...,0])									# find the max value of X
			tY = max(p.coords[...,1])									# find the max value of Y
			if tX>self.maxX: self.maxX=tX 								# if the current poly's max x is smaller than recorded maxX, set min to current
			if tY>self.maxY: self.maxY=tY 
	
	def getExtents(self):
		tMinX=9999
		tMinY=9999
		tMaxX=-9999
		tMaxY=-9999
		# Calculates the spatial extents. ideally this information is calculated in the above for loop, but i'm lazy and this is fast.
		for p in self.records:										# for each poly
			tX = min(p.coords[...,0])									# find the min value of X
			tY = min(p.coords[...,1])									# find the min value of Y
			if tX<tMinX: tMinX=tX 								# if the current poly's min x is smaller than recorded minX, set min to current
			if tY<tMinY: tMinY=tY 								# if the current poly's min y is smaller than recorded miny, set min to current
			tX = max(p.coords[...,0])									# find the max value of X
			tY = max(p.coords[...,1])									# find the max value of Y
			if tX>tMaxX: tMaxX=tX 								# if the current poly's max x is smaller than recorded maxX, set min to current
			if tY>tMaxY: tMaxY=tY 
		return ((tMinX,tMinY),(tMaxX,tMaxY))		

	def shp_shift(self,xSize,ySize):
		''' shp_shift
		desc: 
			changes all of the x and y coordinates of the shapefile and places them within a specified coordainte range from 0-max
		params:
			x: the max x value to scale to 
			y: the max y value to scale to 
		returns:
			nothing: could easily be modified to return a copy of the shapefile instead.
			this may be very important when it comes to automatically computing a geoTiff from the output...
		'''
		# battleplan
		# 1. determine shift forward
		# 2. determine shift back
		# 3. determine scale factor
		# 4. loop each polygon
			# 4.1 shift the poly forward
			# 4.2 shift the poly back
		# 5. optional (return shifted poly)

		#amount to shift forwards is self.minX,self.minY

		for p in self.records:
			#1. shift poly forward by the minX and minY
			p.coords[...,0]+=abs(self.minX)
			p.coords[...,1]+=abs(self.minY)

		#see changes
		ext = self.getExtents()
		logging.debug("step1 poly shift extents"+str(ext))

		for p in self.records:
			#2. shift polygon back to origin
			p.coords[...,0]-=ext[0][0]
			p.coords[...,1]-=ext[0][1]

		#see changes
		ext = self.getExtents()
		logging.debug("step2 poly shift extents"+str(ext))

		for p in self.records:
			#3. stretch over new range of values
			p.coords[...,0]=(p.coords[...,0].astype(float)/ext[1][0])*xSize
			p.coords[...,1]=(p.coords[...,1].astype(float)/ext[1][1])*ySize

		#see changes
		ext = self.getExtents()
		logging.debug("step2 poly shift extents"+str(ext))


class DrawSpatial(object):
	def __init__(self,xSize,ySize):
		self.xSize = xSize
		self.ySize = ySize 
		#self.draw_poly_PNG()

	def shp_poly_mask(self,shp,ind):
		''' desc:
				generate a mask surface for a polygon in the context of the greater shapefile
			params:
				shp -  the shapefile the polygon is situated in 
				i - the index of the polygon in the shapefile
			returns:
				Z : ndarray of the surface data 
		'''
		# transform poly to coordinate space (self.xSize,self.ySize)
		#paint surface
		# draw boundaries
		# fill with empty
		# convert to ndarray
		# return dnarray

		#shp.shp_shift(self.xSize,self.ySize)								#moved this call to the generate command so that I could increase the effiency of algorithm
		data = np.zeros((self.xSize,self.ySize,4),dtype=np.uint8)			#data surface
		surface = cairo.ImageSurface.create_for_data(
			data,cairo.FORMAT_ARGB32,self.xSize,self.ySize)					#the cairo surface to paint on
		cr = cairo.Context(surface)											#cairo context

		cr.set_source_rgb(1.0,1.0,1.0)										#set backgrhound color							
		cr.paint()															#paint background in


		#tell me what poly you are printing out
		logging.info("drawing poly:"+shp.records[ind].name.rstrip())
		start = time.time()

		cr.set_source_rgb(0,0,0)
		# draw a polygon on the cairo surface
		#cr.move_to(shp.records[ind].coords[0,0],shp.records[ind].coords[0,1]-self.ySize)
		#i=1
		for i in range(shp.records[ind].coords.shape[0]-1):
			#cr.move_to(drawCoords[i,0],self.ySize-drawCoords[i,1]) # used 200-Y b/c/ the origin is top left
			cr.line_to(shp.records[ind].coords[i,0],self.ySize-shp.records[ind].coords[i,1]) 	# used 200-Y b/c the origin is top left
		cr.fill()

		#do painting
		cr.set_source_rgba(0, 0, 3, 1)
		cr.set_line_width(3)
		# cr.set_line_rgba(1.0,1.0,1.0)
		cr.stroke()
		cr.close_path()
			
		cr.stroke()
		#don't write to speed up.
		#surface.write_to_png("poly.png") 							#save in current working dir
		
		#size = (int(np.sqrt(w**2+h**2)+1)//4)*4+4 
		# Make an array oput of surface
		buf = surface.get_data()									# raw binary data representing the surface
		Z = np.frombuffer(buf, np.uint8)							# Interpret a buffer as a 1-dimensional array.
		Z.shape = (self.xSize,self.ySize,4)
		y = Z[...,...,0].reshape(self.ySize,self.xSize)									# shape the buffer to the size of the surface in pixels. the array is the values for each pixel from 0-255. empty space is represented by 0
		
		logging.info("poly drawing took:"+str(time.time()-start))
		return y#Z[...,...,0] #self.np_crop(Z,0)									# jump to crop method!

	def shapefile_to_PNG(self,s):
		''' shapefile_to_PNG
		DESC: a method to render the entire shapefile into a PNG 
		Param: s - the shapefile object to render 
		returns: nothing yet.
		'''
		# battleplan:
		# 1. get shapefile object
		# 2. get spatial extents of shapefile and assign min/max variables 
		# 3. shift entire shapefile (save eq'n for later?)
		#---- DO NOT DO HERE # 4. 5 FIRST create a ndarray for the entire shapefile size (shpArray)
		# 5. create a surface for for the entire shapefile
		# 6. createa a copy of the surface/ndarray of the entire space (copyShpArray)
		# 7. start loop for each polygon in shapefile
			# 7.1. create a 2nd copy of the array (2copyshpArray)
			# 7.2. create a mask that blacks out all areas of the 2nd array
			# 7.3. fill wordle into the masked space 
			# 7.4. copy wordle array into the shapefile array (shpArray)
			# 7.5. reset 2CopyArray to zeros
		# 8. set the surface data to shpArray (with correct size)
		# 9. return shpSurface 

		# 1. get shapefile object - should i create a copy here ?
		self.s = s #incomming shapefile
		
		# 2. get extents - CODE MOVED TO Shapefile OBJECT
		logging.info("Before Shift Spatial Extents:\n\t\tmaxY:%f\nminX:%f\t\t\tmaxX:%f\n\t\tminY:%f" %(s.maxY,s.minX,s.maxX,s.minY))

		#3. shift entire polygon (save eq'n for later?)
		s.shp_shift(self.xSize,self.ySize)

		#4. create a ndarray for entire shapefile - SKipping

		#5. draw entire shapefile to surface
		data = p.zeros((self.xSize,self.ySize,4),dtpye=np.uitn8)			#data surface
		surface = cairo.ImageSurface.create_for_data(
			data,cairo.FORMAT_ARGB32,self.xSize,self.ySize)					#the cairo surface to paint on
		cr = cairo.Context(surface)											#cairo context

		cr.set_source_rgb(1.0,1.0,1.0)										#set backgrhound color							
		cr.paint()															#paint it in


		# do wordle for each poly
		for p in s.records:
			# draw the poly outlione onto the output surface
			# create a second surface of size of total and mask out non poly area
			# make wordle in that area
			# add results to the output surface
			pass

	def shapefile_outlines(self,s):
		'''desc:
				this method produces a cairo surface that has all of the polygon outlines drawn inspect
			params:
				s - the hsapefile to Draw 
			returns:
				ndarray - an nd array to add to the final output surface
		'''
		data = np.zeros((self.xSize,self.ySize,4),dtype=np.uint8)
		surface = cairo.ImageSurface.create_for_data(
			data,cairo.FORMAT_ARGB32,self.xSize,self.ySize)
		cr = cairo.Context(surface)
		#cr.setantialias()

		# set bg to white
		cr.set_source_rgb(1.0,1.0,1.0)
		# set bg to black
		cr.set_source_rgb(0.0,0.0,0.0)
		cr.paint()

		logging.info("drawing shapefile poly outlines")

		# draw polygon lines
		#cr.set_source_rgb(0,0,0)
		#draw white
		cr.set_source_rgb(1,1,1)
		cr.set_line_width(2)
		j=0
		for p in s.records:
			for i in range(0,p.coords.shape[0]):
				cr.line_to(p.coords[i,0],self.ySize-p.coords[i,1])
			cr.close_path()
			cr.stroke()
			# j+=1
			# if j<len(s.records):
			# 	cr.move_to(s.records[j].coords[0,0],self.ySize-s.records[j].coords[0,1])
		
		#make the line

		#cr.close_path()
		

		#save output for easy debugging
		# surface.write_to_png("shapefile_outlines.png")
		
		#rip out the data 
		buf = surface.get_data()									# raw binary data representing the surface
		Z = np.frombuffer(buf, np.uint8)							# Interpret a buffer as a 1-dimensional array.
		Z.shape = (self.xSize,self.ySize,4)
		#y = Z[...,...,0].reshape(self.ySize,self.xSize)				# shape the buffer to the size of the surface in pixels. the array is the values for each pixel from 0-255. empty space is represented by 0
		return Z



	def poly_to_PNG(self,p):
		# poly_to_png. renders and saves a polygon object to a cairo surface and a png image file
		self.p = p # an incoming polygon 
		data = np.zeros((self.xSize,self.ySize,4),dtype=np.uint8)
		surface = cairo.ImageSurface.create_for_data(
			data, cairo.FORMAT_ARGB32, self.xSize,self.ySize) # the surface to paint the polygon onto
		cr = cairo.Context(surface) # Cairo Context from surface
		
		# fill with solid white
		cr.set_source_rgb(1.0,2.0,1.0) #set background to white
		cr.paint()	#paint the background in

		# create a shifed polygon array of x,y coordinates
		drawCoords = p.poly_shift(self.xSize,self.ySize)

		#tell me what poly you are printing out
		logging.info("drawing poly:"+p.name)

		# draw a polygon on the cairo surface
		for i in range(drawCoords.shape[0]-1):
			cr.move_to(drawCoords[i,0],self.ySize-drawCoords[i,1]) # used 200-Y b/c/ the origin is top left
			cr.line_to(drawCoords[i+1,0],self.ySize-drawCoords[i+1,1]) # used 200-Y b/c the origin is top left
			

		#do painting
		cr.set_source_rgba(0, 0, 3, 1)
		cr.set_line_width(3)
		# cr.set_line_rgba(1.0,1.0,1.0)
		cr.stroke()
		cr.close_path()
		cr.stroke()
		surface.write_to_png("poly.png") #save in current working dir

	def poly_to_mask(self,p):
		''' desc:
				takes in a poly object and renders to a cairo surface where the inside is white (represented by 0's) and the outside is black (255's)
			params:
				p - a polygon to draw on a surface
			returns:
				nothing, but produces a png of the polygon
		'''

		self.p = p 													# an incoming polygon 
		data = np.zeros((self.xSize,self.ySize,4),dtype=np.uint8)
		surface = cairo.ImageSurface.create_for_data(data, cairo.FORMAT_ARGB32, self.xSize,self.ySize) # the surface to paint the polygon onto
		cr = cairo.Context(surface) 								# Cairo Context from surface
		
		# fill with solid white
		cr.set_source_rgb(1,1,1) 									#set background to black
		cr.paint()													#paint the background in

		# create a shifed polygon array of x,y coordinates
		drawCoords = p.poly_shift(self.xSize,self.ySize)

		#tell me what poly you are printing out
		logging.info("drawing poly:"+p.name)

		cr.set_source(1,1,1) # set to black
		# white cr.set_source_rgb(0,0,0)		
		# draw a polygon on the cairo surface
		#cr.move_to(drawCoords[0,0],drawCoords[0,1]-self.ySize)
		#i=0
		#cr.translate(0,0-self.ySize)
		for i in range(0,drawCoords.shape[0]-1):
			#cr.move_to(drawCoords[i,0],self.ySize-drawCoords[i,1]) # used 200-Y b/c/ the origin is top left
			if drawCoords[i][1]<0:
				logging.debug("this is the problem one? "+str(drawCoords[i][0])+","+str(drawCoords[i][1]))
			else:
				cr.line_to(drawCoords[i,0],self.ySize-drawCoords[i,1]) 	# used 200-Y b/c the origin is top left			
		cr.fill()

		#do painting
		cr.set_source_rgba(0,0,0,1) # set white
		# set to black cr.set_source_rgba(0, 0, 3, 1)
		cr.set_line_width(3)
		# cr.set_line_rgba(1.0,1.0,1.0)
		cr.stroke()
		cr.close_path()
			
		cr.stroke()
		surface.write_to_png("poly.png") 							#save in current working dir
		
		#size = (int(np.sqrt(w**2+h**2)+1)//4)*4+4 
		# Make an array oput of surface
		buf = surface.get_data()									# raw binary data representing the surface
		Z = np.frombuffer(buf, np.uint8)							# Interpret a buffer as a 1-dimensional array.
		Z.shape = (self.xSize,self.ySize,4)
		y = Z[...,...,0].reshape(self.ySize,self.xSize)									# shape the buffer to the size of the surface in pixels. the array is the values for each pixel from 0-255. empty space is represented by 0
		return y#Z[...,...,0] #self.np_crop(Z,0)									# jump to crop method!

class Word(object):
	'''
	Class: Word 
	Desc: a data object class to hold information about words and
	and thier associated weaghts
	#(word,link,weighted weight,parent polygon,topic family number:1-5)
	'''
	def __init__(self,text,link,weight,parent,family,x,y,w,h,angle):
		self.text = text
		self.link = link
		self.weight = weight
		self.parent = parent
		self.family = family
		self.x = x
		self.y = y
		self.w = w
		self.h = h
		self.angle = angle


class Shp_WordCloud(object):
	
	# class: WordCloud
	# Desc: a class that generates a wordcloud on a pyCario surface using numpy array to check for collisions with other words
	def __init__(self,words,_shapefile):
		# Initialize the cloud
		# :Parameters:
		#     `words`: [(word,link,weight),...]
		#         List of words with associated links and relative weights
		

		self.shp = _shapefile
		# Determine minimum and maximum weights
		min_weight = words[0][2]
		max_weight = words[0][2]
		for word in words:
			text, link, weight, parent, family = word #lazy copying error?
			min_weight = min(min_weight, weight) #write min of the current or last min
			max_weight = max(max_weight, weight)# write max of the current or last max

		# Normalize weights and create list of Word objects
		self.words = []
		for word in words:
			text, link, weight, parent, family = word # same as words[i][0]=text, words[i][1]=link, words[i][2]=weight
			weight = (weight-min_weight)/float(max_weight-min_weight) #create decimal version of weight (move towards origin and divide by range)
			self.words.append (Word(text,link,weight,parent,family,-1,-1,0,0,0)) #create a list of Word objects. numbers are x,x,w,h,angle
		
		# Set a color palette for output word colors
		'''self.palette = [(204,204,204,255),
						(153,102,102,255),
						(102,0,0,255),
						(51,0,0,255),
						(102,102,102,255)]'''
		'''self.palette = [(0,191,255,255), 		#sky blue
						(39,64,139,255),		#royal blue 4
						(0,238,118,255),		#spring green
						(255,193,37,255),		#goldenrod
						(199,97,20,255),		#raw sienna
						(238,0,0,255),			#red #1
						(208,32,144,255)]		#violet red
		'''
		self.palette = [(38,226,12,0),
						(248,112,28,0),
						(12,233,184,0),
						(255,0,0,0),						
						(0,144,255,0)]#(250,255,105,255),

		# Font to be used
		self.fontname = 'times'
		self.fontsize_min = 100
		self.fontsize_max = 200

	def np_crop(self,Z, empty=255):
		''' Crop an array by removing empty margins around it

		:Parameters:
			`Z` : numpy array
				 Array to be cropped
			`empty` : scalar
				 Value to be considered empty
		:Return:
			Cropped array, Z center shift
		'''
		if len(Z.shape) == 2:
			Z = Z.reshape (Z.shape+(1,))
		height,width,depth = Z.shape

		x = 0
		while x >= 0 and (Z[:,x,:] == empty).all(): x += 1
		x_left = max(0,x-1)
		x = width-1
		while x >= 0 and (Z[:,x,:] == empty).all(): x -= 1
		x_right = min(width,x+1)
		y = 0
		while y >= 0 and (Z[y,:,:] == empty).all(): y += 1
		y_bottom = max(0,y-1)
		y = height-1
		while y >= 0 and (Z[y,:,:] == empty).all(): y -= 1
		y_top = min(height,y+1)

		dx = -(x_left+(x_right-x_left)//2 - Z.shape[1]//2)
		dy = -(y_bottom+(y_top-y_bottom)//2 - Z.shape[0]//2)
		return Z[y_bottom:y_top:,x_left:x_right], (dx,dy)


	def np_text(self, text, fontname='times', fontsize=48.0, angle=0):
		''' Generate an text within an array

		:Parameters:
			`text` : str
				Text to be generated
			`fontname` : str
				Font family name
			`fontsize` : int
				Font size
			`angle` : float
				Text angle
		'''

		# Dummy surface to get text extents
		surface = cairo.ImageSurface(cairo.FORMAT_A8, 1, 1)
		ctx = cairo.Context(surface)
		ctx.set_font_size(fontsize)
		(x, y, w, h, dx, dy) = ctx.text_extents(text) 		# 6 element tupple - (x_bearing, y_bearing, width, height, x_advance, y_advance)
		size = (int(np.sqrt(w**2+h**2)+1)//4)*4+4 			# ** = power symbol. 2**3 is 2^3. // is integer division
		
		# Actual surface
		surface = cairo.ImageSurface(cairo.FORMAT_A8, size, size) 	#over-write old surface
		ctx = cairo.Context(surface)								#over-write old ctx
		options = cairo.FontOptions()								#creates a FontOptions object with defualt values
		options.set_antialias(cairo.ANTIALIAS_SUBPIXEL)
		options.set_hint_style(cairo.HINT_STYLE_FULL)
		ctx.set_font_options(options)								#use the defualt font options values from above
		ctx.set_source_rgba(0,0,0,0)
		ctx.paint()													#paint blank canvas, essentially
		ctx.set_source_rgba(1,1,1,1)								#change color to white and opaque
		ctx.select_font_face(fontname)		#make font 'sans'
		ctx.set_font_size(fontsize)
		ctx.move_to(size//2,size//2)								#from strange math above ((int(np.sqrt(w**2+h**2)+1)//4)*4+4 )								
		ctx.rotate(angle)
		ctx.rel_move_to(-w//2,h//2)
		ctx.show_text(text)											#draw text on surface

		# Make an array oput of surface
		buf = surface.get_data()									#raw binary data representing the surface
		Z = np.frombuffer(buf, np.uint8)							# Interpret a buffer as a 1-dimensional array.
		Z.shape = (size,size,1)										# shape the buffer to the size of the surface in pixels. the array is the values for each pixel from 0-255. empty space is represented by 0
		return self.np_crop(Z,0)									# jump to crop method!

	def generate(self, width, height):
		''' Generate the cloud on a surface of dimensions width x height
		'''
		self._width = width  
		self._height = height

		# test for efficiency
		self.shp.shp_shift(width,height)

		# Cairo surface that will hold the final result
		self.surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
		ctx = cairo.Context(self.surface)
		options = cairo.FontOptions()
		options.set_antialias(cairo.ANTIALIAS_GRAY)
		options.set_antialias(cairo.ANTIALIAS_SUBPIXEL)

		options.set_hint_style(cairo.HINT_STYLE_FULL)
		ctx.set_font_options(options)
		ctx.set_source_rgba(1,1,1,1)
		ctx.paint()

		Y = np.zeros((height,width,4),dtype=np.int8)
		# numpy array that will serve for collisions tests		
		# Z = self.np_poly

		'''
			THIS IS WHERE TO MAKE THE MAGIC HAPPEN
			i need to make a method that takes the numpy array Z in (all 0's now) and fills all the area outside of the 
			polygon to '255' so it appears as black (or something else)

			i can use the spatial poly method, change the background to black, and the inside to white
			then do Z += polyArray
			Volia. not so hard?

			STEPS:
			1. refine Spatial Poly method for this purpose
			2. return the array of the buffer in identical shape and size to Z
			3. Z +=SpatialPoly

			modified for shp
				-1 create output array 
				1. loop the records
					2. create the masks
					3. create the wordle
					4. write current output to output array
			
			modified for TopicWords
				1. loop polygons
					2.make array of matched polygon words 
					3. loop words
						4. set color based on topic family
		'''		
		#for i in range(0,len(self.records))# production 
		for i in range(0,len(self.shp.records)):#beta # loop every polygon	
		#k=10
		#for i in range(0,k):
			#logging.info("forming cloud:"+str(i+1)+" of "+str(k))							#how many more clouds to go?
			start = time.time()
			logging.info("forming cloud:"+str(i+1)+" of "+str(len(self.shp.records)))							#how many more clouds to go?
			Z = np.zeros((height,width), dtype=np.int8)
			polybuff = DrawSpatial(width,height)										# create a spatial object on a cairo surface
			mask = polybuff.shp_poly_mask(self.shp,i)									# render current shp to mask
			logging.debug("shape of Z:"+str(Z.shape)+ "shape of mask:"+str(mask.shape))	
			Z+=mask

			pWords =[]																	#empty array of words for words to place for specific polygon
			for w in range(len(self.words)):
				#logging.info("comparing "+self.words[w].parent+" to "+self.shp.records[i].name.rstrip())
				if self.words[w].parent ==self.shp.records[i].name.rstrip():				#if the polygon name is the word's parent, add it to the list
					#logging.info("they match!")
					pWords.append(self.words[w])										#add the topicWord to the list to place, else, go to next					
					# if w>5:break														# potential speedup method not used (low number of polys)

			if len(pWords)>0:															#don't bother placing words unless there are words to place
				logging.debug("pWords has length")
				for i in range(len(pWords)):											# loop until all words are placed
					#word = self.words[i]													# depreciated for topicWords # set the current placement word
					#txt,link,weight,x,y,w,h,angle = self.words[i]
					word = pWords[i]

					fontname = self.fontname
					fontsize = self.fontsize_min + \
								int(word.weight*(self.fontsize_max - self.fontsize_min))	# font size related to word weight
					#logging.info("font size is:"+str(fontsize))

					# Choose angle
					angle = 0
					if np.random.uniform() < 0.25:
						angle = -np.pi/2
					#angle = np.random.normal(0.0,0.05)
						
					angle += np.random.normal(0.0,0.025)

					# Get an array with text in it
					z,(dx,dy) = self.np_text(word.text,fontname,fontsize,angle)			# creates a np surface with text extents, then converts to array and is cropped
					h,w,d = z.shape 													# shape is the pixels x piexels required for text and one layer deep
					z.shape = z.shape[:2]												# dump the only 3rd dim now a 2-D array

					hit = 10000															# a maximum try number for each placement. prevents inf. loops?
					r = .01																# rotation angle?
					while hit > 0:

						# Choose a random center
						theta = np.random.random()*np.pi*2
						x = .5+.5*np.cos(theta)*r 										#turn theta into a number for x
						y = .5+.5*np.sin(theta)*r 										#turn theta into a number for y
						r += .005
						x = min(max(int(x*width), w//2), width-1-w//2)					# normalize random x for center
						y = min(max(int(y*height), h//2), height-1-h//2)				# normalize random y for center

						# Test collision
						if (Z[y-h//2:y-h//2+h,x-w//2:x-w//2+w]*z).sum() == 0:					# if Z (output array) is empty in the space that the text's array requires
							Z[y-h//2:y-h//2+h,x-w//2:x-w//2+w] += z 							# fill that area of the array with the contents of the text's array data 
							# r,g,b,a = self.palette[np.random.randint(0, len(self.palette))]	# depreciated for topicwords	# grab some random colors out of the pallete array
							r,g,b,a = self.palette[int(word.family)-1]								#set the color of the word to be consistent for the whole topic family
							ctx.save()															# 
							ctx.select_font_face(fontname)
							ctx.set_font_size(fontsize)
							(xt, yt, wt, ht, tdx, tdy) = ctx.text_extents(word.text)
							ctx.set_source_rgba(g/255.,b/255.,r/255.,1.0)							#make colors
							ctx.move_to(x+dx,y+dy)												#go to place on surface
							ctx.rotate(angle)													#rotate
							ctx.rel_move_to(-wt//2, ht//2)										#move to starting location (from center)
							ctx.show_text(word.text)											#draw text
							ctx.restore()														#not sure
							hit = 0 															#reset hit counter
							word.x = x+dx 														#store positions in object
							word.y = y+dy
							word.w = wt
							word.h = ht
							word.angle = angle
						else:
							word.x, word.y = -1, -1 											#store a failure dummy value
							hit -= 1 															# decrement the number of tries left
			logging.info("cloud took:"+str(time.time()-start))
						
			# Generate a texture for pyglet display
			# this does only takes the cairo surface and makes it work for desktop display
			buf = self.surface.get_data() 															# grab binary data
			Z = np.frombuffer(buf, np.uint8) 														# cast to 1-D array
			
			Z.shape = (height,width,4) 																# shape it for image matrix
			Y +=Z
			# Z = Z[::-1]																				# dump 3rd dim																									
																									# add Z to output array
			#Y=Z[::-1]

		#add in outlines
		X = polybuff.shapefile_outlines(self.shp)
		X.shape = (height,width,4)
		Y +=X

		# change the data in line below to output array
		Y.shape = (height,width,4)

		#turn the data into a surface and save it to file
		logging.info("writing png")
		data = Y
		tS = cairo.ImageSurface.create_for_data(
			data,cairo.FORMAT_ARGB32,width,height)
		cr = cairo.Context(tS)
		tS.write_to_png("Cloud_bug.png")
		
		# REMOVED BECAUSE PYGLET TAKES UP TOO MUCH SWAP SPACE
		#Y=Y[::-1]		
		#self.image = pyglet.image.ImageData(width,height, format='RGBA', data=Y.tostring()) 	# cast to pyglet object


	def save(self):
		'''turned off this code because pyglet couldn't get enough memory.
		Generate a pyglet image from cairo surface
		has nothing to do with operation of algorithm
		'''
		'''buf = self.surface.get_data()
		Z = np.frombuffer(buf, np.uint8)
		width, height = self._width, self._height
		Z.shape = (height,width,4)
		Z = Z[::-1]
		height, width, depth = Z.shape
		image = pyglet.image.ImageData(width,height, format='RGBA', data=Z.tostring())

		filename = 'cloud'
		image.save(filename+'.png')
		f = open(filename+'.map', 'w')
		f.write('<img src="%s" width="%d" height="%d" border="0" usemap="#map" />\n' % (filename+'.png',width,height))
		f.write('<map name="map">\n')
		for i in range(len(self.words)):
			word = self.words[len(self.words)-1-i]
			link = word.link
			if not link:
				link = word.text.replace(' ', '_')+'.html'
			if word.x >= 0 and word.y >=0:
				dx = int(np.cos(word.angle)*word.w/2+np.sin(word.angle)*word.h/2)
				dy = int(np.cos(word.angle)*word.h/2+np.sin(word.angle)*word.w/2)
				x0,y0 = word.x-dx,word.y-dy
				x1,y1 = word.x-dx,word.y+dy
				x2,y2 = word.x+dx,word.y+dy
				x3,y3 = word.x+dx,word.y-dy
				f.write('<area shape="poly" coords="%d,%d,%d,%d,%d,%d,%d,%d" href="%s" />\n' % (
						x0,y0,x1,y1,x2,y2,x3,y3,link))
		f.write('</map>\n')
		f.close()'''


	def on_key_press(self, symbol, modifiers):
		if symbol == pyglet.window.key.SPACE:
			self.generate(self.image.width,self.image.height)
		elif symbol == pyglet.window.key.S:
			self.save()

	def on_draw(self):
		gl.glClearColor(1,1,1,1)
		self.window.clear()
		gl.glColor4f(1,1,1,1)
		w,h = self.image.width, self.image.height
		self.image.blit(x=0,y=0,width=w,height=h)


	def show(self):
		self.generate(10000,10000)
		self.save()
		# currently too big, so just don't print to screen for now...
		# self.window = pyglet.window.Window(self.image.width,self.image.height)
		# gl.glClearColor(0,0,0,1)
		# gl.glBlendFunc (gl.GL_SRC_ALPHA, gl.GL_ONE_MINUS_SRC_ALPHA)
		# gl.glEnable(gl.GL_BLEND)
		# self.window.push_handlers(self)
		# pyglet.app.run()

if __name__ == '__main__':
	
	# j=30
	#s = Shapefile('\\toronto_shp\\NEIGHBORHOODS_WGS84.shp') #tablet directory
	#s = Shapefile('NEIGHBORHOODS_WGS84.shp') #bugaboo directory
	s = Shapefile('van_nhoods.shp') #bugaboo directory
	# logging.info("printing first coordinate of "+s.records[j].name.rstrip()+":"+ str(s.records[j].coords[0]))

	# test the drawing of a poly to a png
	# ShapefilePNG = DrawSpatial(400,400)
	# ShapefilePNG.poly_to_mask(s.records[j])

	# test the drawing of entire shapefile to png
	#shp_png = DrawSpatial(200,200)
	#shp_png.shapefile_to_PNG(s)

	# for i in range(len(s.records)):
	# 	logging.info(str(i)+":"+s.records[i].name)

	# Dummy list of wrods for debugging
	words = [('modernising','www.sfu.ca',380),
		('nonconvertible','www.sfu.ca',471),
		('octupcating','www.sfu.ca',319),
		('bloodstain','www.sfu.ca',332),
		('citrin','www.sfu.ca',423),
		('sawpit','www.sfu.ca',338),
		('secondarily','www.sfu.ca',288),
		('soma','www.sfu.ca',281),
		('subtotalled','www.sfu.ca',190),
		('legislate','www.sfu.ca',467),
		('lordless','www.sfu.ca',500),
		('overglance','www.sfu.ca',437),
		('overattached','www.sfu.ca',384),
		('conquest','www.sfu.ca',299),
		('drowsy','www.sfu.ca',222),
		('enterectomy','www.sfu.ca',310),
		('fueller','www.sfu.ca',420),
		('isochoric','www.sfu.ca',311),
		('killdeer','www.sfu.ca',278),
		('monochromate','www.sfu.ca',224),
		('bloomer','www.sfu.ca',114),
		('teratoma','www.sfu.ca',389),
		('unrighteously','www.sfu.ca',347),
		('protestantism','www.sfu.ca',117),
		('equipage','www.sfu.ca',246),
		('spitefulness','www.sfu.ca',583),
		('frame','www.sfu.ca',127),
		('launder','www.sfu.ca',134),
		('naturally','www.sfu.ca',322),
		('betaine','www.sfu.ca',528),
		('terminableness','www.sfu.ca',324),
		('widgeon','www.sfu.ca',316),
		('prolificacy','www.sfu.ca',251),
		('explicatory','www.sfu.ca',564),
		('squilgeeing','www.sfu.ca',237),
		('gaolbird','www.sfu.ca',590),
		('linotype','www.sfu.ca',383),
		('overlooker','www.sfu.ca',351),
		('boldfacedness','www.sfu.ca',193),
		('tetrahedrite','www.sfu.ca',339),
		('wintertide','www.sfu.ca',515),
		('proteose','www.sfu.ca',472),
		('extemporize','www.sfu.ca',166),
		('staffman','www.sfu.ca',556),
		('generalise','www.sfu.ca',533),
		('lobbyist','www.sfu.ca',348),
		('novosibirsk','www.sfu.ca',589),
		('brasilein','www.sfu.ca',346),
		('tillable','www.sfu.ca',448),
		('witt','www.sfu.ca',484),
		('quinquefoliate','www.sfu.ca',503),
		('saviorship','www.sfu.ca',218),
		('standish','www.sfu.ca',578),
		('haltingness','www.sfu.ca',546),
		('lucency','www.sfu.ca',443),
		('pectinate','www.sfu.ca',557),
		('briza','www.sfu.ca',536),
		('tourbillion','www.sfu.ca',324),
		('rhumba','www.sfu.ca',392),
		('quintuplet','www.sfu.ca',184),
		('electrolyzed','www.sfu.ca',550),
		('suleiman','www.sfu.ca',255),
		('indagate','www.sfu.ca',138),
		('macrolinguistically','www.sfu.ca',536),
		('pidgizing','www.sfu.ca',513),
		('chemistry','www.sfu.ca',290),
		('tropophyte','www.sfu.ca',361),
		('pol','www.sfu.ca',505),
		('rabot','www.sfu.ca',168),
		('semifuturistic','www.sfu.ca',368),
		('superdeposit','www.sfu.ca',265),
		('inhospitality','www.sfu.ca',313),
		('nonassessability','www.sfu.ca',108),
		('anticonservatism','www.sfu.ca',585),
		('choir','www.sfu.ca',268),
		('uncongratulatory','www.sfu.ca',179),
		('postpupillary','www.sfu.ca',132),
		('ragtime','www.sfu.ca',205),
		('sedateness','www.sfu.ca',381),
		('superfinancing','www.sfu.ca',392),
		('hyperchloremia','www.sfu.ca',158),
		('matanuska','www.sfu.ca',551),
		('arcesilaus','www.sfu.ca',573),
		('choreal','www.sfu.ca',105),
		('unfoliaged','www.sfu.ca',578),
		('praefect','www.sfu.ca',479),
		('drawl','www.sfu.ca',583),
		('shovelling','www.sfu.ca',495),
		('gossipred','www.sfu.ca',298),
		('hypertonicity','www.sfu.ca',445),
		('melodeon','www.sfu.ca',434),
		('balance','www.sfu.ca',461),
		('congruency','www.sfu.ca',466),
		('unhypothetical','www.sfu.ca',574),
		('prevaricated','www.sfu.ca',549),
		('dipl','www.sfu.ca',263),
		('subquinquefid','www.sfu.ca',307),
		('godown','www.sfu.ca',135),
		('iconically','www.sfu.ca',510),
		('jamaica','www.sfu.ca',114),
		('myoedema','www.sfu.ca',193),
		('penetralia','www.sfu.ca',227),
		('clotted','www.sfu.ca',154),
		('trihydroxy','www.sfu.ca',187),
		('washbowl','www.sfu.ca',538),
		('cuneately','www.sfu.ca',245),
		('subrostral','www.sfu.ca',309),
		('guffaw','www.sfu.ca',432),
		('kasavubu','www.sfu.ca',115),
		('naumachiae','www.sfu.ca',255),
		('petrification','www.sfu.ca',404),
		('compatriotism','www.sfu.ca',177),
		('unsnubbed','www.sfu.ca',447),
		('yestreen','www.sfu.ca',599),
		('deny','www.sfu.ca',437),
		('slier','www.sfu.ca',322),
		('gummed','www.sfu.ca',225),
		('kinnikinic','www.sfu.ca',242),
		('oversubtle','www.sfu.ca',383),
		('phineas','www.sfu.ca',498),
		('carissimi','www.sfu.ca',519),
		('unsophistication','www.sfu.ca',169),
		('radiation','www.sfu.ca',371),
		('depend','www.sfu.ca',225),
		('stot','www.sfu.ca',537),
		('haemangiomata','www.sfu.ca',222),
		('lavishment','www.sfu.ca',479),
		('noncumbrous','www.sfu.ca',239),
		('alcimedes','www.sfu.ca',459),
		('consecrater','www.sfu.ca',441),
		('unheroic','www.sfu.ca',249),
		('ridotto','www.sfu.ca',514),
		('duad','www.sfu.ca',209),
		('sungrebe','www.sfu.ca',559),
		('heartener','www.sfu.ca',269),
		('liquidized','www.sfu.ca',339),
		('nonprovided','www.sfu.ca',394),
		('alwin','www.sfu.ca',260),
		('sushi','www.sfu.ca',398),
		('unmisunderstandable','www.sfu.ca',150),
		('pollinosis','www.sfu.ca',596),
		('extraneously','www.sfu.ca',320),
		('supportably','www.sfu.ca',517),
		('heparinize','www.sfu.ca',276),
		('loss','www.sfu.ca',377),
		('npl','www.sfu.ca',471),
		('ana','www.sfu.ca',461),
		('turishcheva','www.sfu.ca',153),
		('unmodern','www.sfu.ca',319),
		('population','www.sfu.ca',346),
		('famine','www.sfu.ca',463),
		('fecal','www.sfu.ca',225),
		('hypermarket','www.sfu.ca',288),
		('neurophysiology','www.sfu.ca',414),
		('nursery','www.sfu.ca',409),
		('blabbermouth','www.sfu.ca',291),
		('unarticulatory','www.sfu.ca',490),
		('unriddle','www.sfu.ca',537),
		('prefatorial','www.sfu.ca',562),
		('rudolph','www.sfu.ca',254),
		('goldfishes','www.sfu.ca',516),
		('impertinentness','www.sfu.ca',403),
		('matronizing','www.sfu.ca',436),
		('ocellated','www.sfu.ca',470),
		('beloved','www.sfu.ca',154),
		('synaesthetic','www.sfu.ca',156),
		('unseizable','www.sfu.ca',257),
		('preaggressive','www.sfu.ca',294),
		('sepulchral','www.sfu.ca',202),
		('flaggiest','www.sfu.ca',569),
		('interdetermined','www.sfu.ca',344),
		('mullein','www.sfu.ca',277),
		('outvoicing','www.sfu.ca',580),
		('buzzardly','www.sfu.ca',493),
		('tonga','www.sfu.ca',118),
		('valuate','www.sfu.ca',383),
		('crosspiece','www.sfu.ca',323),
		('shaef','www.sfu.ca',120),
		('forthrightness','www.sfu.ca',307),
		('inveiglement','www.sfu.ca',386),
		('kalamazoo','www.sfu.ca',563),
		('minutest','www.sfu.ca',355),
		('pauline','www.sfu.ca',473),
		('backsliding','www.sfu.ca',368),
		('unadvantaged','www.sfu.ca',321),
		('undeserved','www.sfu.ca',206),
		('differentiated','www.sfu.ca',108),
		('shreadhead','www.sfu.ca',115),
		('frapping','www.sfu.ca',588),
		('kindredship','www.sfu.ca',566),
		('mooresville','www.sfu.ca',595),
		('phalanger','www.sfu.ca',591),
		('bizonia','www.sfu.ca',324),
		('tachypneic','www.sfu.ca',596),
		('undialled','www.sfu.ca',287),
		('dipsomania','www.sfu.ca',469),
		('skyjack','www.sfu.ca',388),
		('gamma','www.sfu.ca',454),
		('looey','www.sfu.ca',595),
		('overvary','www.sfu.ca',256),
		('abhominable','www.sfu.ca',273),
		('blas','www.sfu.ca',100),
		('tehran','www.sfu.ca',562),
		('unexudative','www.sfu.ca',149),
		('eching','www.sfu.ca',589),
		('solidness','www.sfu.ca',595),
		('gleipnir','www.sfu.ca',164),
		('loury','www.sfu.ca',476),
		('nondefinite','www.sfu.ca',376),
		('actinic','www.sfu.ca',188),
		('baudikin','www.sfu.ca',401),
		('tetanic','www.sfu.ca',443),
		('unhoaxed','www.sfu.ca',285),
		('expectant','www.sfu.ca',381),
		('spicing','www.sfu.ca',341),
		('habitably','www.sfu.ca',401),
		('lutose','www.sfu.ca',447),
		('nonerudition','www.sfu.ca',291),
		('afterburning','www.sfu.ca',312),
		('boride','www.sfu.ca',414),
		('toadless','www.sfu.ca',173),
		('vestiary','www.sfu.ca',594),
		('sarouk','www.sfu.ca',500),
		('stumming','www.sfu.ca',260),
		('harvestfish','www.sfu.ca',427),
		('mangrum','www.sfu.ca',550),
		('nonreversibility','www.sfu.ca',522),
		('amphithura','www.sfu.ca',393),
		('canephore','www.sfu.ca',251),
		('touraco','www.sfu.ca',331),
		('preplotting','www.sfu.ca',280),
		('scamper','www.sfu.ca',213),
		('supercrowned','www.sfu.ca',428),
		('hearties','www.sfu.ca',147),
		('mccormack','www.sfu.ca',240),
		('northerly','www.sfu.ca',130),
		('alkalimetric','www.sfu.ca',545),
		('cantillation','www.sfu.ca',495),
		('trouveur','www.sfu.ca',514),
		('rededicated','www.sfu.ca',476),
		('facilely','www.sfu.ca',254),
		('greaseproof','www.sfu.ca',220),
		('hectogram','www.sfu.ca',329),
		('memberless','www.sfu.ca',447),
		('oligophrenic','www.sfu.ca',564),
		('antinomic','www.sfu.ca',257),
		('caulker','www.sfu.ca',439),
		('uncorrupted','www.sfu.ca',331),
		('defecating','www.sfu.ca',562),
		('rollable','www.sfu.ca',116),
		('fibrillose','www.sfu.ca',562),
		('hiveless','www.sfu.ca',165),
		('metioche','www.sfu.ca',147),
		('outshrill','www.sfu.ca',520),
		('acuminated','www.sfu.ca',357),
		('cavaedium','www.sfu.ca',596),
		('unswooning','www.sfu.ca',175),
		('decancellate','www.sfu.ca',373),
		('sent','www.sfu.ca',472),
		('franchiser','www.sfu.ca',513),
		('incarceration','www.sfu.ca',177)]



	#real words from postgres database
	tableName = 'geoLDA_van_complete'
	t = TopicWords(tableName)
	words = t.words
	
	#generate the wordcloud - polygon
	# cloud = WordCloud(words,s.records[j])
	# cloud.show()

	#generate wordcloud for polygon
	cloud = Shp_WordCloud(words,s)
	cloud.show()





	#print s.records[0]
