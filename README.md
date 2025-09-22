# Warcraftlogs data-gathering

This program connects to the warcraftlogs API to collect data. The data will be saved as separate .parquet files for each run.

## How to run

### Warcraftlogs authentication
The warcraftlogs API uses OAuth 2.0 for authentication. This means that you have to get yourself a token for authenticating.
Follow the link here for the setup: https://www.warcraftlogs.com/api/docs
The functions in the program will assume there is a .env file where it can find the client_ID, client_secret and WARCRAFTLOGS_TOKEN.
```
client_ID = EXAMPLE123 # <<-- you enter this (no brackets)
client_secret = EXAMPLE456 # <<-- you enter this (no brackets)
WARCRAFTLOGS_TOKEN =  # The program will enter a string in single quotation here ('example')
```


* Warcraftlogs uses Oauth 2.0 for authentication to it's API, if you want to access it you can follow the guide here: https://www.warcraftlogs.com/api/docs
    * Put the client_ID and client_secret in a .env file for safety.
* The program uses user ID's for warcraftlogs to get a list of reports to download. If you want to change which ID's to use, find the "get_report_codes" function and change list of user ID's to include the ID's you want. 


## Background
The popular game World of Warcraft can be played in many diffrent ways. For groups of five the most popular way is to do "dungeons". 
Most of the users doing dungons aims to do a type of dungeons called "mythic plus". These dungons have a number attached to them, and a higher number means a more difficult dungeon. Only if you complete the dungeon within a timelimit will you be able to enter a more difficult one. 

Users can download "mods" that allows them to save the data from the dungeon and upload it to a website called warcraftlogs. 
Each batch of data is called a "report" (sometimes a "log"), and within each report there can be data for multiple dungeons (or "fights").
The site warcraftlogs has tools for looking at the data and make rankings for how you performed in a dungeon compared to others. 

My goal with this project was to build a tool that allows the user to look at some of the parts that warcraftlogs cares less about: personal performence and how you perform with others. What players have you played the most with? Do you succeed more often with some? What are your highs and lows in respective dungeons?


## The 
