# YOUTUBE DATA HARVESTING & WAREHOUSING
This project is about extractingthe channel, video and comment info and analysing the collected data. The program uses the YouTube Data API to retrieve the required data such as channel information, video satistics and comment information

**characteristics of the Application**
1. Fetch Youtube Channel Information such as channel ID, Channel Name, channel Typem Channel Views and Description
2. Fetch video details such as Video ID, Video Name, Like count, duration, published date, video caption and comments count
3. Fetch comment information such as comment id, video id, comment author and comment date
4. save the retrieved data to Database (MySQL)
5. SQL Query output will be diplayed as Tabel in streamlit application
6. Enables users to search for channel details and join tables to view data in the Streamlit app.

**Prerequisites**
1. Python
2. Youtube Data API
3. python Libraries like streamlit, pandas, MySql and google API
4. MySQL

**Setup**
1. Activate Youtube API in Google console and notedown the API key
2. Assign the youtube api key to the variable api_key in the code
3. make sure MySQL is installed and running

**usage**
1. The Application has two tabs, Home Tab and Query Tab
2. Home tab has the baisc information about the application and a input box to enter the channel ID.
3. The 'Get Info' button will helps to retrive the  channel related information.
4. The 'Load to DB' button is used to fetch and add the channel, video and comment data of the respective channel to MySQL Database.
5. The query tab has a list of questions for which the results need to be produced.
6. select the respective query and click result button to see the output.


      
