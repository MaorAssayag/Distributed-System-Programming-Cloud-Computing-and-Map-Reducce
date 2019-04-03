import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

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
 * LocalApp class - represent the Local Application
 * For now preUpload class need to be run before this file (to upload jars&scripts).
 */
public class LocalApp {

    final static Tag TAG_MANAGER = new Tag("name","manager");
    private static String LocalAppID;

    public static void main( String[] args ){

        // General vars
        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = false;

        /** 1. if you want to terminate the manager args = inputFileName outputFileName n terminate */
        if (args.length > 3 && args[3].equals("terminate"))
            terminate = true;

        // Promotion
        System.out.println("**************************************************************** \n");
        System.out.println(
                "_____/\\\\\\\\\\\\\\\\\\______/\\\\\\______________/\\\\\\______/\\\\\\\\\\\\\\\\\\\\\\___        \n" +
                        " ___/\\\\\\\\\\\\\\\\\\\\\\\\\\___\\/\\\\\\_____________\\/\\\\\\____/\\\\\\/////////\\\\\\_       \n" +
                        "  __/\\\\\\/////////\\\\\\__\\/\\\\\\_____________\\/\\\\\\___\\//\\\\\\______\\///__      \n" +
                        "   _\\/\\\\\\_______\\/\\\\\\__\\//\\\\\\____/\\\\\\____/\\\\\\_____\\////\\\\\\_________     \n" +
                        "    _\\/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\___\\//\\\\\\__/\\\\\\\\\\__/\\\\\\_________\\////\\\\\\______    \n" +
                        "     _\\/\\\\\\/////////\\\\\\____\\//\\\\\\/\\\\\\/\\\\\\/\\\\\\_____________\\////\\\\\\___   \n" +
                        "      _\\/\\\\\\_______\\/\\\\\\_____\\//\\\\\\\\\\\\//\\\\\\\\\\_______/\\\\\\______\\//\\\\\\__  \n" +
                        "       _\\/\\\\\\_______\\/\\\\\\______\\//\\\\\\__\\//\\\\\\_______\\///\\\\\\\\\\\\\\\\\\\\\\/___ \n" +
                        "        _\\///________\\///________\\///____\\///__________\\///////////_____\n");
        System.out.println(" Distriduted System Programming : PDF Document Conversion in the Cloud");
        System.out.println(" By Maor Assayag & Refahel Shetrit \n");
        System.out.println(" Stage 1|    Local AWS App has been started \n");

        // Initialize mAWS object and get a random UUID
        LocalAppID = UUID.randomUUID().toString();
        mAWS myAWS = new mAWS();
        myAWS.initEC2();
        myAWS.initS3();
        String managerID;
        try {
            /**2. Start a Manager instance on EC2 (if its not already running) */
            managerID = checkManager(myAWS);
            if(managerID != null) {
                // Promotion
                System.out.println(" Stage 2|    Manager instance already running, Manager ID : " + managerID + "\n");
            }
            else{
                managerID = startManager(myAWS);
                // Promotion
                System.out.println(" Stage 2|    Manager instance has been started, Manager ID : " + managerID + "\n");
            }


            /** 3. Upload the input file to S3 bucket */
            String uploadedFileURL = uploadFileToS3(myAWS, inputFileName);
            System.out.println(" Stage 3|    The input file has been uploaded to " + uploadedFileURL + "\n");


            /** 4. Send the uploaded file URL to the SQS queue*/
            myAWS.initSQS();
            send2SQS(myAWS, n, terminate);
            if (terminate){
                System.out.println(" Stage 4|    Terminate message has been sent to the SQS queue \n");
            }else{
                System.out.println(" Stage 4|    The file URL has been sent to the SQS queue \n");
            }


            /** 5. Wait & Receive the response from the Manager instance for the operation that has been requested*/
            System.out.println(" Stage 5|    Waiting for response from the Manager...");
            waitForAnswer(myAWS, LocalAppID, 2500);
            System.out.println("             Response from the Manager is ready in S3 storage \n");


            /** 6. Download the operation summary file from S3 & Create an HTML file representing the results*/
            String outputFilePath = downloadResult(myAWS, outputFileName);
            System.out.println(" Stage 6|    Summary file received in the Local AWS App \n");
            System.out.println("             HTML file representing the results has been created on " + outputFilePath + "\n");


            /** 7. Send a terminate message to the manager if it received terminate as one of the input arguments*/
            if (terminate) {
                endManager(myAWS, managerID, 2500);
                System.out.println(" Stage 7|    Manager has been terminated as requested \n");
                System.out.println(" Stage 8|    Local AWS App finished with terminating the Manager");
            }else
                System.out.println(" Stage 7|    Local AWS App finished without terminating the Manager");

            System.out.println(" _______________   __________ \n" +
                    " ___  ____/___  | / /___  __ \\\n" +
                    " __  __/   __   |/ / __  / / /\n" +
                    " _  /___   _  /|  /  _  /_/ / \n" +
                    " /_____/   /_/ |_/   /_____/");
            System.out.println("****************************************************************\n");

        } catch (AmazonServiceException e) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + e.getMessage());
            System.out.println("HTTP Status Code: " + e.getStatusCode());
            System.out.println("Error Type:       " + e.getErrorType());
            System.out.println("AWS Error Code:   " + e.getErrorCode());
            System.out.println("Request ID:       " + e.getRequestId());
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");

        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * startManager - method used to start a manager if a manager isn't active
     * Recommended AMI image which supports user-data : ami-51792c38
     * Currently using T2 instances are Burstable Performance Instances that provide
     * a baseline level of CPU performance with the ability to burst above the baseline.
     *
     * Image ID : ami-0080e4c5bc078760e - Linux 64 bit with full support of java
     *
     * @return the id of a manager instance that has been created
     */
    private static String startManager(mAWS aws) {
        ArrayList<String> managerInstance = aws.initEC2instance("ami-0080e4c5bc078760e", 1, 1, InstanceType.T2Micro.toString(),
                Header.MANAGER_SCRIPT, Header.INSTANCE_KEY_NAME, TAG_MANAGER);
        return 	managerInstance.get(0);
    }

    /**
     * checkManager - method used to determined if the local app needs to start a new Manager
     * instance in aws.
     * @return instanceID if manager found, else null
     */
    private static String checkManager(mAWS aws) {
        return aws.getEC2instanceID(TAG_MANAGER, "running");
    }

    /**
     * uploadFileToS3 - method that uploads the input file from the user to be read, distributed &
     * executed by a running manager.
     * @param inputFileName location of the file to upload to S3
     * @param aws amazon web service object with EC2 & S3
     * @return path (url) of the uploaded file in S3 - a confirmation of successful upload
     */
    private static String uploadFileToS3(mAWS aws, String inputFileName) {
        return aws.mUploadS3(Header.INPUT_BUCKET_NAME, LocalAppID, new File(inputFileName));
    }

    /**
     * send2SQS - method that send a message to Amazon Simple Queue Service :
     * A Fully managed message queues for microservices, distributed systems, and serverless applications.
     * The method initialize a Queue of messages (if needed) and send a message with the input file's URL and
     * information about the number of workers, LocalAppID etc.
     *
     * @param aws amazon web service object with EC2 & S3
     * @param terminate doest we want to send a termination message
     * @param n number of workers expected
     */
    private static void send2SQS(mAWS aws, int n, boolean terminate) {
        String queueURL = aws.initSQSqueues(Header.INPUT_QUEUE_NAME, "0");
        String msg = LocalAppID + Header.MESSAGE_DIFF + terminate + Header.MESSAGE_DIFF + n;
        aws.sendSQSmessage(queueURL, msg);
    }

    /**
     * waitForAnswer - method thats check the SQS in the cloud until there's a message
     * associate with the LocalAppID (meaning the Manager has responded and finished/stop
     * processing the requested operation)
     * @implNote Blocking-IO method that sleeps for 'sleep' ms
     * @param aws amazon web service object with EC2 & S3
     * @param key UUID key associate with this instance of Local application (global LocalAppId)
     * @param sleep the amount of time in ms between searching for answer in the SQS
     */
    private static void waitForAnswer(mAWS aws, String key, int sleep) {
        List<Message> messages;
        String queueUrl = aws.initSQSqueues(Header.OUTPUT_QUEUE_NAME, "0");
        System.out.println("LocalApp waiting for response from " + Header.OUTPUT_QUEUE_NAME);

        while(true) {
            messages = aws.receiveSQSmessage(queueUrl); // Receive List of all messages in queue
            for (Message message : messages) {
                if(message.getBody().equals(key)) {
                    String myMessage = message.getReceiptHandle();
                    aws.deleteSQSmessage(Header.OUTPUT_QUEUE_NAME, myMessage); // Delete the message from the queue
                    return;
                }
            }
            // "busy"-wait
            try {Thread.sleep(sleep);}
            catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * downloadResult - method that downloads the summary file from S3 for creating
     * an html file representing the results.
     * @param aws amazon web service object with EC2 & S3
     * @return result - a list of lines(String) from the format:
     *                  '<operation>: input file output file'
     */
    private static String downloadResult(mAWS aws, String outputFileName) {
        S3Object bucket = aws.mDownloadS3file(Header.OUTPUT_BUCKET_NAME, LocalAppID);
        InputStream inputStream  = bucket.getObjectContent();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String outputFilePath = null;
        String line;

        try (PrintWriter out = new PrintWriter(outputFileName + ".HTML", "UTF-8")){

            out.println("<html>\n<body>");

            while ((line = bufferedReader.readLine()) != null)
                out.println(line + "<br>");
            bufferedReader.close();

            out.println("</body>\n</html>");
            outputFilePath = new java.io.File(".").getCanonicalPath() + File.separator + outputFileName + ".html";

        } catch (Exception ex){
            ex.printStackTrace();
        }

        aws.mDeleteS3file(Header.OUTPUT_BUCKET_NAME, LocalAppID);
        return outputFilePath;
    }

    /**
     * endManager - terminate the Manager Instance on EC2 service
     * @param myAWS mAWS amazon web service object with EC2, S3 & SQS
     * @param MangerID - EC2 Manger instance ID
     * @param sleep the amount of time in ms between searching for answer in the SQS
     */
    private static void endManager(mAWS myAWS, String MangerID, int sleep) {
        waitForAnswer(myAWS, Header.TERMINATED_STRING, sleep);
        myAWS.terminateEC2instance(MangerID);
    }
}

