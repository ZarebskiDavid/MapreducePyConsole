#!/usr/env/bin python
import sys, getpass
from fabric2 import Connection

# mrpyconsole
# Warning: 	depends on fabric v2.0, not fabric v1.0 -> sudo pip3 install fabric2

def ConnectNode(u, p, h):
	print("----> Connecting to ", h)
	global connect

	try:
		connect = Connection(host=h, user=u ,connect_kwargs={"password":p})
	except : 
		print("Wrong ssh connexion parameters. Please check also that you are using fabric v2.0") # trouver un moyen de faire printer cela
	return(connect)

def FindJar():
	global jar_path
	print("----> Fetching hadoop-streaming*.jar....")
	 
	jar_path = connect.run("find /home /usr -name 'hadoop-streaming*[^a-z].jar'").stdout[:-1] #(output -> str -> -1 to remove \n)

	if jar_path=="":
		raise ValueError('Could not find any hadoop-streaming.jar on host')
	return(jar_path)
	

def FindBin(bin):
	print("----> Fetching " + bin + " .... ")
	 
	path = connect.run("find /home /usr -path '*/bin/"+bin+"'").stdout[:-1] #(output -> str -> -1 to remove \n)

	if path=="":
		raise ValueError('Could not find '+ bin +' on host')
	return(path)


def RunMapReduce(m, r, jar_path, yarn_path, hdfs_path, u):
	try: 
		connect.run('rm -R temp_streaming')
	except:
		print('')
	try:
		connect.run('mkdir temp_streaming')
		connect.put(m, "temp_streaming/mapper.py")
		connect.put(r, "temp_streaming/reducer.py")
	except RuntimeError:
		print("Could not upload Mapper and Reducer. Please check your files and the rights of your user")

	input_path = input("Please choose the input of your program (on your HDFS File System) ")
	output_path = input("Please choose a name for your output folder (e.g. /nameOfMyFolder) ")

	#main_command = str(yarn_path+ ' jar '+ jar_path+' -files /home/'+u+'/temp_streaming/mapper.py,/home/'+u+'/temp_streaming/reducer.py -mapper /home/'+u+'/temp_streaming/mapper.py -reducer /home/'+u+'/temp_streaming/reducer.py -input '+ input_path + ' -output '+ output_path)

	main_command = str(yarn_path+ ' jar '+ jar_path+' -files /home/'+u+'/temp_streaming/mapper.py,/home/'+u+'/temp_streaming/reducer.py -mapper mapper.py -reducer reducer.py -input '+ input_path + ' -output '+ output_path)

	
	try:	
		connect.run(main_command , echo=True)

	except RuntimeError:
		print("Something went wrong")
	
	connect.run(hdfs_path+ ' dfs -get ' +output_path +'/* /home/'+u+'/temp_streaming/') # get files from HDFS 
	
	keep_results_on_host = input("would you like to keep these results on your HDFS File System (yes/No) ")
	
	if keep_results_on_host == "" or keep_results_on_host == "no" or keep_results_on_host == "No":
		connect.run(hdfs_path+ ' dfs -rm -R ' +output_path )

	results = connect.run('ls temp_streaming').stdout.split('\n')[:-1]  # I know, this is ugly

	try:
		for f in results: 
			connect.get('temp_streaming/'+f)
	except:
		print('----> Results importation error')
 
	print('----> ' + str(results)+ ' saved in current folder')


def Disconnect():
	print('Disconnecting')
	connect.run('rm -R temp_streaming') 
	connect.close()

def mrpyconsole():
	try:
		host_string = str(sys.argv[1])
		user = str(sys.argv[2])
		mapper = str(sys.argv[3])
		reducer = str(sys.argv[4])
		password = getpass.getpass(prompt='Password for '+user+': ', stream=None)
		ConnectNode(user,password ,host_string)
		FindJar()
		yarn_path=FindBin("yarn")
		hdfs_path=FindBin("hdfs")
	
		RunMapReduce(mapper, reducer, jar_path, yarn_path, hdfs_path, user)
	
		Disconnect()
		print("Done")
	except IndexError:
		print("Wrong syntax: mrpyconsole [host] [user] [mapper] [reducer] \n ---- [host]: ip or domain name of your hadoop master node (e.g. 192.168.0.37)\n ---- [user]: ssh user of server (e.g. dav)\n ---- [mapper]: path to your python mapper (e.g. /somewhere/over/the/rainbow/mapper.py) \n ---- [reducer]: path to your python reducer (e.g. /somewhere/over/the/rainbow/reducer.py)")


if __name__ == '__main__':
	mrpyconsole()