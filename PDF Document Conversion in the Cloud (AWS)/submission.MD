<img src="http://codigodelsur.com/wp-content/uploads/2016/02/AmazonWebservices_Logo.svg_.png" width="250"><img src="https://acrobat.adobe.com/content/dam/doc-cloud/images/acrobat/how-to/create-pdf/adc-fd-create-marquee-1440x200-en.jpg" width="130">  

# Distributed System Programming Cloud Computing and Map Reducce  
## Assignment 1 - PDF Document Conversion in the Cloud (AWS)  


### Authors
*Maor Assayag* - ID 318550746  
Computer Engineer, Ben-gurion University, Israel

*Refhael Shetrit* - ID 204654891  
Computer Engineer, Ben-gurion University, Israel

#
### General
In this assignment we will code a real-world application to distributively process a list of PDF files, perform some operations on them,and display the result on a web page (HTML file).  

**System architecture** and the assignment requirements are fully described in the github page
(its compose mainly from the assignment official page and from our additions) -  

https://github.com/MaorAssayag/Distributed-System-Programming-Cloud-Computing-and-Map-Reducce/tree/master/PDF%20Document%20Conversion%20in%20the%20Cloud%20(AWS)  

and this file in more nice way to read it (.MD) -  

https://github.com/MaorAssayag/Distributed-System-Programming-Cloud-Computing-and-Map-Reducce/blob/master/PDF%20Document%20Conversion%20in%20the%20Cloud%20(AWS)/submission.MD

In this doucment we will elaborate on technical matters that we thought should be emphasized and on the substantive questions that were taken into account during the planning & design process.  

#
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

#
### S3 buckets format
To be able to easily distinguish Local App requests we are creating for each LocalApp a new bucket in the following format :
*"dsp-192-local-app-" + "UUID"* (first 12 digits of random 64-bit)  
(AWS S3 service require uniq names for new buckets that at most 33 chars long).  

Each LocalApp bucket contains *input-folder* (the input file from the user), *output-folder* (new converted files from the worker) and *result-file.txt* from the Manager to be sent to the LocalApp.

In addition, there is a constant bucket named *mr-dsp-192-pre-upload-jars-scripts* that contains the Worker & Manager Scripts & Jars files and the ManagerApp Logger file if the Manager has been terminated (by error / LocalApp request) named *ManagerLogger.txt*.

#
### SQS queues
#### Queues
As described in the system architecture, queues are used for:

* communication between the local application and the manager.
* communication between the manager and the workers. 

In order to allow optimal running times and supports better scalability we seperate the messages to 5 main queues as follow :  

* queue from LocalApp to ManagerApp - *Header.INPUT_QUEUE_NAME* - *inputQueue*
* queue from Manager to Manager threads - *Header.INPUT_THREAD_QUEUE_NAME - *inputThreadsQueue*
* queue from Manager to Workers - *Header.INPUT_WORKERS_QUEUE_NAME* - *inputWorkerQueue*
* queue from Workers to Manager - *Header.OUTPUT_WORKERS_QUEUE_NAME* - *outputWorkerQueue*
* queue from Manager to LocalApp - *Header.OUTPUT_QUEUE_NAME* - *outputQueue*  

#### Messages format
* from *Local app* to the *Manager* and from the *Manager* to the *Manager inner Threads*:
     * message = *LocalAppID* + " " + *terminate* + " " + *n* + " " + *uploadedFileURL*
     * parsedMessage[0] = localAppID, first 12 is shortLocalAppID
     * parsedMessage[1] = terminate, true/false
     * parsedMessage[2] = n , number of workers so the LocalApp will compute locally how many workers needed **(numOfPdfs / n)**, with the limit of 19 *Workers* as mention above.
     * parsedMessage[3] = uploadedFileURL, input file URL in S3   
     
* from the *Workers* to the *Manager* :
    * message = outputLine = *operation* + ":" + "\t" + *pdfURL* + "\t" + *resultsURL* or *Error message*;
    * parsedMessage[0] = operation, describe what to do with the first page of the PDF - *ToImage / ToHTML / ToText*
    * parsedMessage[1] = pdfURL. the input file orginial URL online
    * parsedMessage[2] = the *result file URL* on the LocalApp S3 bucket \ short desription of an *Exception* if occurred during conversion
  
* from the *Manager* to the *Local App* :
    * final reult message = *shortLocalAppID* (first 12 chars) + " " + *resultURL*
    * final terminate message ACK = *Terminated* + *shortLocalAppID* (first 12 chars)                            

#
### Scalability
Currently all instaces are running on *T2.micro* type which is the free-tire that amazon provides.  

If we are talking about how many users (LocalApps) the Manager can handles / create num *n* of Workers to run in parallel, we need
to take into account the compute power of the instace type.  

Our system architecture enables us to scale up as much as the compute power that amazon can provide or as much as we are ready to put into the budget.  

Eventully its comes to how many LocalApps your local computer can run, and how many Workers you want to supports.

Currently in LocalApp.java we are limit the user to ask for more then 19 worker instances (+1 for the ManagerApp instance) due to the free-tire limits of 20 instances from Amazon AWS.

In addition, in *ManagerApp.java* the ThreadPoolExceture handles up to *10* threads to serve new requests from LocalApp's (the main thread listening to new requests and for each one sending them to the excetuer to be serve when possible by the thread pool).  

We limit the thread pool to 10 due to low compute power of *T2.micro* and the fact that each task as serving a new LocalApp request consdider to take some time (an acceptable assumption when it comes to IO (W/R files) inquiries and network (Upload/Download files/messages) inquiries).

#
### Persistence
We design our code to handle all fail-cases in the right manner and situation, to keep the system running until something mandatory to stop occurs.  

With each new LocalApp request to the Manager the current Active workers nodes are updated globaly (to all the threads that handles requests - needed for sudden termination of workers checking).  
We make sure to have an updated view on how many workers still up when the user required *x* amount of workers.  

If a worker suddnely dies without sendind to the *output worker queue* the results to the request he pulled, the request will simply become visible after some time-out will ran out (currently 10s) to other workers that still running.  

If a worker failed due to communiction problems (which did not arise from the content of the request itself) - the worker will not delete the request to mark it as done, and will keep trying to pulling requests from the *input wokeres queue*.  

The Manager is waiting to get responses for each requests from the *input-file*. To supports scalabilities we don't limit the amount of time the Manager is waiting for all the respones, but after *num_of_lines x 1s / num_of_workers* we are checking if there is still active worker to detecet sudden termination of nodes.  

All stages in the Manager is been logged in a Logger file which is uploaded to the *mr-dsp-192-pre-upload-jars-scripts* bucket as *ManagerLogger.txt* file when the Manager is exsiting (due to Error/LocalApp request from the user) to furter review if required.

#
### Threads
When we looking at improve scalability and timing, we need to think about integrate threads in our application mainly at the Manager code, which containing the bottleneck of our application.  

The Manager needs to received new requests from Local Apps **and** handle the collecting of Workers respons to upload final-result files for each LocalApp requests.  

To achieve this in *ManagerApp.java* a ThreadPoolExceture handles up to *10* threads to serve new requests from LocalApp's (while the main thread listen to new requests and for each one sending them to the excetuer to be serve when possible by the thread pool). We have already elaborate as required on why *10* threads above.  

We tested the integrity of the system concurrency by running several local applications (each one for a different *input file*) and montoring the Manager Logger, SQS queues, S3 buckets & EC2 instances online.  

Its important to note that this system architecture is all based on distributed system programming and cloud computing utilizing AWS services.  By distributing storage and computation across many instances / buckets, the resource can grow with demand while remaining economical at every size.

#
### Termination process
The *local app* can send with the request for a new input-file a terminate message to the *Manager*.  

Once the *Manager* send the new request to be handle in the thread pool, eventually a thread will start handling this request.  

Once done, the thread will stop the *Manager* for continue receiving new requests from *Local Apps*, will terminate the *thread pool executor* and start terminating all the *Worker instances*.  

This thread will not finish until he gets a confirmation from AWS services about all the instances status.  

After a confirmation will be received the thread (in the name of the Manager main thread) will upload the *Manager Logger* file to S3 and will send a confirmation SQS message to the *Local App* that made the request to terminate.

Once the *Local App* (waiting for answear from the *Manager* to be terminated) received the terminate ACK message the *Local App* will terminate the *Manager* instance.

#
### Distribution
#### Diterbuted work among Workers
While the manager creates a node for every n messages, it does not delegate messages to specific nodes. All of the worker nodes take their messages from the same SQS queue - inputWorkerQueue; so it might be the case that with 2n messages, hence two worker nodes, one node processed n+(n/2) messages, while the other processed only n/2.  

The difference in performance for each *Worker* is mainly due to differences in reaction times from the various sites \ PDF size and the content / action required to perform.  

The assignment of requests to a specific *Worker* and an equal distribution in advance will harm the principles of decentralization and the overall timing of the system.

#### Blocking sections
The integration of threads in the *Manager* allow it to receiving new requests from *LocalApps* while other threads handling the requests separately - elmentaing the blocking of the main bottleneck of the system.  

The *Workers* runs smoothly - get new request from the *Manager*, convert the file, upload the results and keep listening to new requests.  

Finally, the *Local App* is blocked until an answer will be sent from the *Manager* - accordingly to the system architecture.

#
### Running the Application
#### Updating jars/scripts files in S3
Everytime you change the *LocalApp.java*, *ManagerApp.java*, *Worker.java*, *Header.java* or *mAWS.java* files you should upload the Manager jar & Worker jar to S3.

Currently all jars & scripts files are in the S3 bucket named *"mr-dsp-192-pre-upload-jars-scripts"*.   

Nothing is hard coded, and everything is easliy changeable in the Header.java file.  

In *LocalApp.java* there is 2 booleans in the Main class - overwriteJars & overwriteScripts which is *false* by default, simply change them to *true* to be able to upload the jars when running *LocalApp*.

In *LocalApp.java* **uploadJars** and **uploadScripts** methods update the path to the jars files in your project.

Update the vars in Header.java that conatining uniqe names , e.g. pre-upload bucket (LocalApp bucket conatining random UUID so no worry there).

#### How to create the jars files ?
We build our projcet in Intellij on java 1.6 (max version supported by AWS EC2 instances).  

There you can build jars files from Java files using *Build Artifacts* with auto detect which jars needed in the package ~100MB, [for more information.](https://www.jetbrains.com/help/idea/working-with-artifacts.html)  

The scripts are simple Base64 text files represting [Bash scripts](https://linuxhint.com/bash_base64_encode_decode/).  

####  Set up your AWS 
* Create **KeyPair** and download the private key to test locally (running Local App) in managerInstance.pem (a text file containting the private key from AWS), [for more information](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html).  
You can download AWS tool-kit in Intellij (tool-kit store) just to make sure the credentials are working properly.

* Create **IAM role**, set a name (prefer same as the KeyPair name) to it and give it to new EC2 instances when initalizing them (*mAWS.java, initEC2instance method, String KeyName*). Make sure that you give this IAM role fully access permissions to SQS, EC2 and S3. [for more information](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html).  

#### Example of input file / output file
* [input file e.g.]()
* [output file e.g.]()

#### Run
in CMD :

        java -jar LocalApp.jar inputFileName outputFileName n [terminate]
or run in Intellij once configuring a new running configuration.

#
## Thanks.
