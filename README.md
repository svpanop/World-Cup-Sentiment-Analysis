Project Name (TBD)
===================

----------

Description
-----------
In fringilla ex a dui tincidunt auctor. Cras egestas vulputate sem ut aliquet. Maecenas sollicitudin commodo quam imperdiet eleifend. Morbi eget enim sed nulla ullamcorper faucibus. Donec sodales nisi risus, eu vehicula metus efficitur ut. Curabitur sit amet augue efficitur, maximus ipsum in, gravida dui. Vestibulum pellentesque ultricies nisl, at faucibus dui scelerisque non. Donec libero nulla, volutpat non consequat eu, maximus sodales odio.

----------

Installation
----------

 - **Prerequisites:**
HDP(or Sandbox)  2.1 and Flume installed.
Maven for building Flume sources.


----------


 - **Buid Flume Sources:**
	 Navigate to Twitter folder(location where *pom.xml* is).
	 
	Issue the command: `mvn clean package`.
	
	Copy `/target/twitter-search-source-1.0-SNAPSHOT` to *flume_lib* location in HDP node. It's probably in `/usr/lib/flume/lib`.


----------


	
 - **Mess with flume configuration file:** 
 
	 Flume configuration file for downloading Tweets is in `/Twitter` folder.
	 Update values and upload in  HDP node.
	 
	 An easy location is `/usr/lib/flume/conf`


----------

 - **Get some Tweets:**
	 Open an ssh connection with the HDP node you want to download tweets and issue the command:
    `flume-ng agent --conf-file /etc/flume/conf/flume.conf --name TwitterAgent`

	**NOTE**
	If process hangs after displaying this:
	`INFO twittersource.TwitterSearchSource: remaining: 0`
	Hit Control + C.
	
----------


    

	 