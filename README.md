<img src="https://acc.amityaump.com/acc-content/uploads/2019/02/cloud-computing.png" width="400">

# Distributed System Programming Cloud Computing and Map Reducce

### Abstract
A course from Computer Science Department in Ben-gurion Unviesity of the Negev, Israel.  
Modern Web-scale applications (e.g., Facebook, Twitter, Google Docs) must face technical challenges that derive from their scale:  
* Scalability: the possibility to grow as the user-base and data-size handled by the application grows to hundreds of millions of users and petabytes of data.  
* High-Availability: the capacity to provide service to users even when part of the infrastructure (CPUs, Networks, Disks) become unaccessible in an intermittent or permanent manner.  

The way to address these requirements is to develop loosely distributed applications that can operate in a "cloud-like" runtime environment.  

This course introduces basic theory behind such massively distributed applications and modern programming tools that constitute an emerging infrastructure for distributed applications.
#
  
  
<img src="http://codigodelsur.com/wp-content/uploads/2016/02/AmazonWebservices_Logo.svg_.png" width="200">

### First Assignment - PDF Document Conversion in the Cloud
#### Abstract
In this assignment you will code a real-world application to distributively process a list of PDF files, perform some operations on them, and display the result on a web page.  

The application is composed of a local application and instances running on the Amazon cloud. The application will get as an input a text file containing a list of URLs of PDF files with an operation to perform on them. Then, instances will be launched in AWS (workers). Each worker will download PDF files, perform the requested operation, and display the result of the operation on a webpage.
The use-case is as follows:  
* User starts the application and supplies as input a file with URLs of PDF files together with operations to perform on them, an integer n stating how many PDF files per worker, and an optional argument terminate, if received the local application sends a terminate message to the Manager.  
* User gets back an html file containing PDF files after the result of the operation performed on them. 

#
### Authors
*Maor Assayag*  
Computer Engineer, Ben-gurion University, Israel

*Refhael Shetrit*  
Computer Engineer, Ben-gurion University, Israel
