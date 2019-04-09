import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Distributed System Programming : Cloud Computing and Map-Reducce1 - 2019/Spring
 * Assignment 1
 *
 * DSP Local Application
 * PDF Document Conversion in the Cloud
 *
 * Creators : Maor Assayag
 *            Refahel Shetrit
 *
 * ManagerApp class
 */
public class ManagerApp {

    private static Logger logger = Logger.getLogger(ManagerApp.class.getName());
    private static HashMap<String, String> myAWSsqsURL;
    private static Tag TAG_WORKER = new Tag("name","worker");
    private static mAWS myAWS;
    private static String resultURL;
    private static boolean keepAlive = true;
    private static ArrayList<String> instancesId = new ArrayList<String>();
    private static int currWorkers = 0;

    public static void main(String[] args){
        try {
            initLogger("ManagerLogger");
            logger.info(" Stage 1|    Manager App has been started on EC2 instance\n");
            final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            List<Message> messages = new ArrayList<Message>();
            Message message;

            myAWS = new mAWS(false);
            myAWS.initAWSservices();
            initializeAllQueues(myAWS);
            logger.info(" Stage 2|    Start listening to the following queue : " + Header.INPUT_QUEUE_NAME + "\n");

            while(keepAlive) {

                while (messages.isEmpty()) {
                    messages = get1MessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), 0);
                    if (!keepAlive){
                        break;
                    }
                    try {Thread.sleep(Header.sleep);}
                    catch (InterruptedException e){logger.warning(e.toString());}
                }
                if (!keepAlive) {
                    break;
                }
                logger.info(" General|    Attaching a thread to handle new task request from a Local App \n");
                message = messages.get(0);

                // transfer this message from the input queue to the threads queue
                myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), message.getReceiptHandle());
                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), message.getBody());
                messages.clear();

                Runnable newTask = new Runnable() {
                    @Override
                    public void run() {
                        try{
                            List<Message> messagesThread = new ArrayList<Message>();
                            while (messagesThread.isEmpty()) {
                                messagesThread = get1MessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), 60);

                                try {Thread.sleep(Header.sleep);}
                                catch (InterruptedException e){logger.warning(e.toString());}
                            }
                            Message messageCurr = messagesThread.get(0);
                            //messagesThread.clear();

                            /*
                             * message = LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL
                             * parsedMessage[0] = localAppID - first 12 is shortLocalAppID
                             * parsedMessage[1] = terminate - true/false
                             * parsedMessage[2] = n - number of workers
                             * parsedMessage[3] = uploadedFileURL - input file URL in S3
                             */
                            String[] parsedMessage = messageCurr.getBody().split(" ");
                            logger.info(" Stage 3|    Retrieved the following message from a Local App: \n");
                            logger.info("             " + messageCurr.getBody() + "\n");

                            // check how many workers is currently running under the tag workers
                            currWorkers = checkWorkers(myAWS);
                            int n;
                            try {
                                n = Integer.parseInt(parsedMessage[2]);
                            }
                            catch (NumberFormatException e) {
                                n = 0;
                            }

                            // create the (n-currWorkers) instances of Workers with the tag Worker
                            if (n - currWorkers > 0){
                                logger.info("             Adding " + (n-currWorkers) + " instances of Workers" + "\n");
                                instancesId.addAll(myAWS.initEC2instance(Header.imageID,
                                        1,
                                        (n-currWorkers),
                                        InstanceType.T2Micro.toString(),
                                        Header.PRE_UPLOAD_BUCKET_NAME,
                                        Header.WORKER_SCRIPT,
                                        Header.INSTANCE_WORKER_KEY_NAME,
                                        TAG_WORKER));
                                currWorkers = checkWorkers(myAWS);
                            }
                            logger.info("             Thread handling this task\n");
                            logger.info(" Stage 4|    Analyzing the following input file : " + parsedMessage[3] + "\n");
                            resultURL = analyzeTextFile(myAWS, parsedMessage[0].substring(0,12), parsedMessage[3]);

                            logger.info("\n Stage 5|    Computing complete, Sending the following message to the output queue : \n");
                            logger.info("               " + parsedMessage[0].substring(0,12) + " " + resultURL + "\n");
                            myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), parsedMessage[0].substring(0,12) + " " + resultURL);

                            // Delete the message from the thread queue
                            myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), messageCurr.getReceiptHandle());
                            logger.info(" Stage 6|    Busy-wait to new messages..." + "\n");

                            // Check if terminate message has been received
                            if (Boolean.parseBoolean(parsedMessage[1])){
                                logger.info(" Stage 6|    Terminate request received, terminating "+ instancesId.size() + " workers instances..."  + "\n");

                                // Terminate all workers instances start by this Manager
                                myAWS.terminateEC2instance(instancesId);

                                // Stop retrieving messages from the input queue, and wait for stopping the running
                                keepAlive = false;

                                // Stop the thread pool executor
                                executor.shutdown();

                                // Terminate the worker instances
                                try {Thread.sleep(500);}
                                catch (InterruptedException e){logger.warning(e.toString());}

                                while (checkWorkers(myAWS) > 0){
                                    instancesId.addAll(myAWS.getEC2instancesByTagState(TAG_WORKER, "running"));
                                    instancesId.addAll(myAWS.getEC2instancesByTagState(TAG_WORKER, "pending"));
                                    myAWS.terminateEC2instance(instancesId);

                                    try {Thread.sleep(Header.sleep);}
                                    catch (InterruptedException e){logger.warning(e.toString());}
                                }

                                // Upload the Manager Logger file
                                try{
                                    myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.LOGGER_FILE_NAME, new File("ManagerLogger.txt"));
                                }catch (Exception e){
                                    System.out.println("Error while uploading Manager logger + " + e.toString());
                                }

                                // Send terminate message ack to the local app that asked it
                                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), Header.TERMINATED_STRING + parsedMessage[0].substring(0,12));
                            }

                        }catch (AmazonServiceException ase) {
                            logger.warning("Caught an AmazonServiceException, which means your request made it "
                                    + "to Amazon S3, but was rejected with an error response for some reason.");
                            logger.warning("Error Message:    " + ase.getMessage());
                            logger.warning("HTTP Status Code: " + ase.getStatusCode());
                            logger.warning("AWS Error Code:   " + ase.getErrorCode());
                            logger.warning("Error Type:       " + ase.getErrorType());
                            logger.warning("Request ID:       " + ase.getRequestId());

                        } catch (AmazonClientException ace) {
                            logger.warning("Caught an AmazonClientException, which means the client encountered "
                                    + "a serious internal problem while trying to communicate with S3, "
                                    + "such as not being able to access the network.");
                            logger.warning("Error Message: " + ace.getMessage());

                        } catch (Exception e){
                            logger.warning(e.toString());
                        } finally {
                            // if the Manager has been terminated before getting to this section this file will be
                            // uploaded in the thread that got the termination message
                            try{
                                myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.LOGGER_FILE_NAME, new File("ManagerLogger.txt"));
                            }catch (Exception e){
                                System.out.println("Error while uploading Manager logger + " + e.toString());
                            }
                        }
                    }
                };

                // Attach a thread to handle this task
                executor.execute(newTask);
            }
            logger.info(" Stage 7|    Manager has finished by terminate request from Local App" + "\n");

        } catch (AmazonServiceException ase) {
            logger.warning("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.warning("Error Message:    " + ase.getMessage());
            logger.warning("HTTP Status Code: " + ase.getStatusCode());
            logger.warning("AWS Error Code:   " + ase.getErrorCode());
            logger.warning("Error Type:       " + ase.getErrorType());
            logger.warning("Request ID:       " + ase.getRequestId());

        } catch (AmazonClientException ace) {
            logger.warning("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.warning("Error Message: " + ace.getMessage());

        } catch (Exception e){
            logger.warning(e.toString());

        }
    }

    /**
     * initializeAllQueues - initialize the required queue for the Manager operation
     * @param myAWS mAWS amazon web service object with EC2, S3 & SQS
     */
    private static void initializeAllQueues(mAWS myAWS) {
        ArrayList<Map.Entry<String, String>> queues = new ArrayList<Map.Entry<String,String>>();
        // queue from LocalApp to ManagerApp
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_QUEUE_NAME, "0"));

        // queue from Manager to Manager threads
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_THREAD_QUEUE_NAME, "0"));

        // queue from Manager to Workers
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_WORKERS_QUEUE_NAME, "0"));

        // queue from Workers to Manager
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_WORKERS_QUEUE_NAME, "0"));

        // queue from Manager to LocalApp
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_QUEUE_NAME, "0"));

        myAWSsqsURL = myAWS.initSQSqueues(queues);
    }

    /**
     * analyzeTextFile - Manager creates an SQS message for each URL and operation from the input list.
     * Then, the thread that handles this task is waiting for the workers to response with the results
     * of those requests
     * ( Manager reads all Workers messages from SQS and creates one summary file,
     * once all URLs in the input file have been processed).
     *
     * @param myAWS mAWS amazon web service object with EC2, S3 & SQS
     * @param shortLocalAppID the LocalApp ID that request this input-file
     * @param inputFileURL the input-file URL in S3 LocalApp bucket
     * @return outputURL, the result-file URL on S3 LocalApp bucket
     */
    private static String analyzeTextFile(final mAWS myAWS, String shortLocalAppID, String inputFileURL){
        String outputURL = null;
        java.util.logging.Logger
                .getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.SEVERE);

        try {
            // get the input file from the user
            URI fileToBeDownloaded = new URI(inputFileURL);
            AmazonS3URI s3URI = new AmazonS3URI(fileToBeDownloaded);
            S3Object inputFile = myAWS.mDownloadS3file(s3URI.getBucket(), s3URI.getKey());

            InputStream inputStream  = inputFile.getObjectContent();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            logger.info("             Sending all the requests from the input file to workers \n             ");
            // Sending all input file lines to the workers input queue
            String inputLine;
            int count = 0;
            while ((inputLine = bufferedReader.readLine()) != null){
                logger.info(""+count);
                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.INPUT_WORKERS_QUEUE_NAME), shortLocalAppID  + "\t" + inputLine);
                count++;
            }
            bufferedReader.close();

            // Create new result file
            File file = new File(Header.RESULT_FILE_NAME);
            PrintWriter out = new PrintWriter(file, "UTF-8");
            List<Message> currMessages;
            logger.info("\n             Fetching messages from workers \n             ");

            // Schedule checking for sudden termination
            new java.util.Timer().schedule(new TimerTask(){
                @Override
                public void run() {
                    logger.info("Schedule checking for sudden termination");
                    int curr = checkWorkers(myAWS);
                    if (curr < currWorkers){
                        logger.info("             Adding " + (currWorkers-curr) + " instances of Workers to total of " + currWorkers +" Workers \n");
                        instancesId.addAll(myAWS.initEC2instance(Header.imageID,
                                1,
                                (currWorkers-curr),
                                InstanceType.T2Micro.toString(),
                                Header.PRE_UPLOAD_BUCKET_NAME,
                                Header.WORKER_SCRIPT,
                                Header.INSTANCE_WORKER_KEY_NAME,
                                TAG_WORKER));
                        currWorkers = checkWorkers(myAWS);
                    }
                    // after *num_of_lines x 1s / num_of_workers* we are checking if there is still active worker to detect sudden termination of nodes.
                }
            },1000*(count+1)/(currWorkers+1),1000*(count+1)/(currWorkers+1));


            // Waiting for workers to process all the requests
            while(count > 0){
                currMessages = myAWS.receiveSQSmessage(myAWSsqsURL.get(Header.OUTPUT_WORKERS_QUEUE_NAME));
                for(Message message : currMessages){
                    // add this result from the worker to the Result-file
                    out.println(message.getBody());

                    // delete this message from the output worker queue
                    myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.OUTPUT_WORKERS_QUEUE_NAME), message.getReceiptHandle());

                    // decrease the count - when we done processing enough messages from the workers we exit
                    count--;
                    logger.info(""+count);
                }
                // "busy"-wait for 0.5 second while workers keep completing other requests
                try {Thread.sleep(Header.sleepFetchingFromWorkers);}
                catch (InterruptedException e){
                    logger.warning(e.toString());
                }
            }
            out.close();

            // Upload File file to app_bucket+LocalID S3 and return the URL
            outputURL =  myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, null, Header.RESULT_FILE_NAME, file);

        }catch (Exception e){
            logger.warning(e.toString());
        }
        return outputURL;
    }

    /**
     * checkWorkers - check how many instances of workers is running\pending, used to make sure
     * how much workers nodes currently/soon will operate on new requests
     * @param myAWS mAWS amazon web service object with EC2, S3 & SQS
     * @return instanceID if manager found, else null
     */
    private static int checkWorkers(mAWS myAWS) {
        return myAWS.getNumEC2instancesByTagState(TAG_WORKER, "running") +
                myAWS.getNumEC2instancesByTagState(TAG_WORKER, "pending");
    }

    /**
     * get1MessageFromSQS - get 1 message from a queue with visibility Time-Out
     *
     * @param myAWS mAWS amazon web service object with EC2, S3 & SQS
     * @param queueURL which queue we want to read from
     * @param visibilityTimeOut how much time in seconds the message should be invisible in the queue
     * @return the message
     */
    private static List<Message> get1MessageFromSQS(mAWS myAWS, String queueURL, int visibilityTimeOut) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);

        // Retrieve 1 message
        receiveMessageRequest.setMaxNumberOfMessages(1);

        // Make the current message invisible for visibilityTimeOut seconds
        receiveMessageRequest.setVisibilityTimeout(visibilityTimeOut);

        return myAWS.receiveSQSmessage(receiveMessageRequest);
    }

    /**
     * initLogger - init logger file of the Manager operation. this file will be uploaded to the pre-upload
     * bucket on S3 when this ManagerAPP is terminate.
     *
     * @param loggerName - just the name of the file
     * @throws IOException
     */
    private static void initLogger(String loggerName) throws IOException{
        FileHandler fileHandler = new FileHandler(loggerName + ".txt");
        fileHandler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
        logger.addHandler(fileHandler);
    }
}
