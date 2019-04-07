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
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Distriduted System Programming : Cloud Computing and Map-Reducce1 - 2019/Spring
 * Assignment 1
 *
 * DSP Local Application
 * PDF Document Conversion in the Cloud
 *
 * Creators : Maor Assayag
 *            Refahel Shetrit
 *
 * ManagerApp class - The code that will run by the Manager instance on EC2
 * for now only creating a simple bucket for example
 */
public class ManagerApp {

    private static Logger logger = Logger.getLogger(ManagerApp.class.getName());
    private static HashMap<String, String> myAWSsqsURL;
    private static Tag TAG_WORKER = new Tag("name","worker");

    public static void main(String[] args){

        System.out.println(" Stage 1|    Manager App has been started on EC2 instance\n");
        List<Message> messages = new ArrayList<Message>();
        ArrayList<String> instancesId = new ArrayList<String>();
        String[] parsedMessage;
        String resultURL;
        int n = 0;
        boolean keepAlive = true;
        int currWorkers = 0;

        try {
            mAWS myAWS = new mAWS(true);
            myAWS.initAWSservices();
            initializeAllQueues(myAWS);
            System.out.println(" Stage 2|    Start listening to the following queue : " + Header.OUTPUT_QUEUE_NAME + "\n");

            while(keepAlive) {

                while (messages.isEmpty()) {
                    messages = get1MessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_QUEUE_NAME));

                    try {Thread.sleep(Header.sleep);}
                    catch (InterruptedException e){e.printStackTrace();}
                }
                Message message = messages.get(0);
                messages.clear();
                /*
                 * message = LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL
                 * parsedMessage[0] = localAppID - first 12 is shortLocalAppID
                 * parsedMessage[1] = terminate - true/false
                 * parsedMessage[2] = n - number of workers
                 * parsedMessage[3] = uploadedFileURL - input file URL in S3
                 */
                parsedMessage = message.getBody().split(" ");
                System.out.println(" Stage 3|    Retrieved the following message from a Local App: \n");
                System.out.println("             " + message.getBody() + "\n");

                // check how many workers is currently running under the tag workers
                currWorkers = checkWorkers(myAWS);
                try {
                    n = Integer.parseInt(parsedMessage[2]);
                }
                catch (NumberFormatException e) {
                    n = 0;
                }

                // create the (n-currWorkers) instances of Workers with the tag Worker
                if (n - currWorkers > 0){
                    System.out.println("             Adding " + (n-currWorkers) + " instances of Workers" + "\n");
                    instancesId.addAll(myAWS.initEC2instance(Header.imageID,
                            1,
                            (n-currWorkers),
                            InstanceType.T2Micro.toString(),
                            Header.PRE_UPLOAD_BUCKET_NAME,
                            Header.WORKER_SCRIPT,
                            Header.INSTANCE_WORKER_KEY_NAME,
                            TAG_WORKER));
                }

                System.out.println(" Stage 4|    Analyzing the following input file : " + parsedMessage[3] + "\n");
                resultURL = analyzeTextFile(myAWS, parsedMessage[0].substring(0,12), parsedMessage[3]);

                System.out.println("\n Stage 5|    Computing complete, Sending the following message to the output queue : \n");
                System.out.println("               " + parsedMessage[0].substring(0,12) + " " + resultURL + "\n");
                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), parsedMessage[0].substring(0,12) + " " + resultURL);

                // Delete the message from the queue
                myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), message.getReceiptHandle());
                System.out.println(" Stage 6|    Busy-wait to new messages..." + "\n");

                // if the user want to terminate, terminate all the workers instances
                if (Boolean.parseBoolean(parsedMessage[1])){
                    // terminate all workers instances start by this Manager
                    myAWS.terminateEC2instance(instancesId);

                    // send terminate message ack to the local app asked it
                    myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), Header.TERMINATED_STRING + parsedMessage[0].substring(0,12));

                    // stop retrieving messages from the input queue, and wait for stopping the running
                    keepAlive = false;
                }

                // "busy"-wait for the next try to get a new message
                try {Thread.sleep(Header.sleep);}
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());

        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void initializeAllQueues(mAWS myAWS) {
        ArrayList<Map.Entry<String, String>> queues = new ArrayList<Map.Entry<String,String>>();
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_QUEUE_NAME, "0"));
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_WORKERS_QUEUE_NAME, "0"));
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_WORKERS_QUEUE_NAME, "0"));
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_QUEUE_NAME, "0"));
        myAWSsqsURL = myAWS.initSQSqueues(queues);
    }

    public static String analyzeTextFile(mAWS myAWS, String shortLocalAppID, String inputFileURL){
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

            System.out.println("             Sending all the requests from the input file to workers \n             ");
            // Sending all input file lines to the workers input queue
            String inputLine;
            int count = 0;
            while ((inputLine = bufferedReader.readLine()) != null){
                System.out.print(".");
                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.INPUT_WORKERS_QUEUE_NAME), shortLocalAppID  + "\t" + inputLine);
                count++;
            }
            bufferedReader.close();

            // Create new result file
            File file = new File(Header.RESULT_FILE_NAME);
            PrintWriter out = new PrintWriter(file, "UTF-8");
            List<Message> currMessages;
            System.out.println("             Fetching messages from workers \n             ");

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
                    System.out.print(".");
                }
                // "busy"-wait for 0.5 second while workers keep completing other requests
                try {Thread.sleep(Header.sleep);}
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
            out.close();

            // Upload File file to app_bucket+LocalID S3 and return the URL
            outputURL =  myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, null, Header.RESULT_FILE_NAME, file);

        }catch (Exception e){
            e.printStackTrace();
        }
        return outputURL;
    }


    /**
     * checkWorkers
     * @return instanceID if manager found, else null
     */
    private static int checkWorkers(mAWS myAWS) {
        return myAWS.getEC2instancesByTagState(TAG_WORKER, "running") +
                myAWS.getEC2instancesByTagState(TAG_WORKER, "pending");
    }

    /**
     *
     * @param myAWS
     * @param queueURL
     * @return
     */
    private static List<Message> get1MessageFromSQS(mAWS myAWS, String queueURL) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);

        // Retrieve 1 message
        receiveMessageRequest.setMaxNumberOfMessages(1);

        // Make the current message invisible for 20s
        receiveMessageRequest.setVisibilityTimeout(20);

        return myAWS.receiveSQSmessage(receiveMessageRequest);
    }

    private static void initLogger(String shortLocalAppID) throws IOException {
        FileHandler fileHandler = new FileHandler(shortLocalAppID + ".txt");
        fileHandler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
        logger.addHandler(fileHandler);
    }
}
