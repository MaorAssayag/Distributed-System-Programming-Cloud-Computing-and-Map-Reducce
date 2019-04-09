# Distributed System Programming Cloud Computing and Map Reducce
## PDF Document Conversion in the Cloud (AWS)

### Authors
*Maor Assayag* - ID 318550746  
Computer Engineer, Ben-gurion University, Israel

*Refhael Shetrit* - ID 204654891  
Computer Engineer, Ben-gurion University, Israel

### General
In this assignment we will code a real-world application to distributively process a list of PDF files, perform some operations on them,and display the result on a web page (HTML file).  

Full architecture and the assignment requirements are fully described in the github page
(its compose mainly from the assignment official page and from our additions) -  

https://github.com/MaorAssayag/Distributed-System-Programming-Cloud-Computing-and-Map-Reducce/tree/master/PDF%20Document%20Conversion%20in%20the%20Cloud%20(AWS)  

and this file in more nice way to read it (.MD) -  

https://github.com/MaorAssayag/Distributed-System-Programming-Cloud-Computing-and-Map-Reducce/blob/master/PDF%20Document%20Conversion%20in%20the%20Cloud%20(AWS)/submission.MD

### Security
Its important to diffrincate between Local Apps running locally and apps runing on cloud instaces.  

#### Locally
We used *new ProfileCredentialsProvider().getCredentials()* to get the credentials from a local .pem file conatining the private key
from a keyPair that we created online on AWS console.  

Then, we can attach this credentials to a new EC2 object which will be used later to initalize new EC2 instances while adding to the request the name of the KeyPair (*RunInstancesRequest.withKeyNAme*).  

Without accessing the AWS console online nobody can retreive this private key and use it.  

#### On the cloude - ManagerApp / Workers
We created a new IAM role - An IAM role is an IAM identity that you can create in your account that has specific permissions.  
An IAM role is similar to an IAM user, in that it is an AWS identity with permission policies that determine what the identity can and cannot do in AWS.  

With this role we can initlaize new EC2 instance with specific permissions without exposing any private crednitals by adding the following *request.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(keyName))*.  

In the IAM role we can detrmine that only LocalApp from our user (using the KeyPair) can use this role Name and grant permissions to new instaces on EC2 (e.g. permissions to the Manager to Fully accessed SQS, EC2 and S3).

### S3 buckets
To be able to easily distinguish Local App requests we are creating for each LocalApp a new bucket in the following format :
*dsp-192-local-app- + (first 12 digits of random 64-bit UUID)*  
(AWS S3 service require uniq names for new buckets that at most 33 chars long).  

Each LocalApp bucket contains *input-folder* (the input file from the user), *output-folder* (new converted files from the worker) and *result-file.txt* from the Manager to be sent to the LocalApp.

In addition, there is a constant bucket named *mr-dsp-192-pre-upload-jars-scripts* that contains the Worker & Manager Scripts & Jars files and the ManagerApp Logger file if the Manager has been terminated (by error / LocalApp request) named *ManagerLogger.txt*.

### Scalability
Currently all instaces are running on *T2.micro* type which is the free-tire that amazon provides.  

If we are talking about how many users (LocalApps) the Manager can handles / create num *n* of Workers to run in parallel, we need
to take into account the compute power of the instace type.  

Our system architecture enables us to scale up as much as the compute power that amazon can provide or as much as we are ready to put into the budget.  

Eventully its comes to how many LocalApps your local computer can run, and how many Workers you want to supports.

Currently in LocalApp.java we are limit the user to ask for more then 19 worker instances (+1 for the ManagerApp instance) due to the free-tire limits of 20 instances from Amazon AWS.

In addition, in *ManagerApp.java* the ThreadPoolExceture handles up to *10* threads to serve new requests from LocalApp's (the main thread listening to new requests and for each one sending them to the excetuer to be serve when possible by the thread pool).  

We limit the thread pool to 10 due to low compute power of *T2.micro* and the fact that each task as serving a new LocalApp request consdider to take some time (an acceptable assumption when it comes to IO (W/R files) inquiries and network (Upload/Download files/messages) inquiries).



### How to run the program

    What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!
    Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!
    Did you run more than one client at the same time? Be sure they work properly, and finish properly, and your results are correct.
    Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.
    Did you manage the termination process? Be sure all is closed once requested!
    Did you take in mind the system limitations that we are using? Be sure to use it to its fullest!
    Are all your workers working hard? Or some are slacking? Why?
    Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!
    Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another? 
