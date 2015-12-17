Twitter Events Detection

This is a Spark Streaming project that detects Earthquakes by listening to the Twitter Streaming API.
This is a locality based event detection which achieves temporal grouping by performing windowed operations. 
It uses the Stanford NER clissifier to first detect locations from the text and then extracts location from location names by using a TwoFishes Geocoder which has to be supplied.

How to run:

1) In IntelliJ IDEA, import an SBT project from the cloned directory 

2) Change the twitter credentials in the main function on ServiceBootstrap class

3) Setup the Twofishes Geocoding server. Download server jar and index from twofishes.net

	java -jar server-assembly-0.84.9.jar --hfile_basepath 2015-03-05-20-05-30.753698/

4) Specify the URL of the TwoFishes Geocoding Server

