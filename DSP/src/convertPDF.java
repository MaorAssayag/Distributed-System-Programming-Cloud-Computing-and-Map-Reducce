import com.amazonaws.services.ec2.model.Tag;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.pdfbox.util.PDFText2HTML;
import java.io.*;
import java.net.URL;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.InstanceType;
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
 * convertPDF class - temp class for checking & PDF conversions
 */

public class convertPDF {

    public static final String IMAGE_FORMAT = "png";
    public static final String IMAGE_NAME = "pdf";
    public static final String TEXT_NAME = "pdf";
    public static final String HTML_NAME = "pdf";
    public static final String RESULT_NAME = "result";
    public static final String ENCODING = "UTF-8";
    final static Tag TAG_MANAGER = new Tag("Type","Manager");
    private static String LocalAppID;

    public static void main(String[] args) {
        //String inputFileURL = "https://www.cs.bgu.ac.il/~dsps192/wiki.files/sampleInput.txt";
        //String resultsPath = analyzeTextFile(inputFileURL);
        //System.out.println(resultsPath);
        //testText2HTML("text2HTMLtest");

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
                "_____/\\\\\\\\\\\\\\\\\\______/\\\\\\______________/\\\\\\_____/\\\\\\\\\\\\\\\\\\\\\\___        \n" +
                " ___/\\\\\\\\\\\\\\\\\\\\\\\\\\___\\/\\\\\\_____________\\/\\\\\\___/\\\\\\/////////\\\\\\_       \n" +
                "  __/\\\\\\/////////\\\\\\__\\/\\\\\\_____________\\/\\\\\\__\\//\\\\\\______\\///__      \n" +
                "   _\\/\\\\\\_______\\/\\\\\\__\\//\\\\\\____/\\\\\\____/\\\\\\____\\////\\\\\\_________     \n" +
                "    _\\/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\___\\//\\\\\\__/\\\\\\\\\\__/\\\\\\________\\////\\\\\\______    \n" +
                "     _\\/\\\\\\/////////\\\\\\____\\//\\\\\\/\\\\\\/\\\\\\/\\\\\\____________\\////\\\\\\___   \n" +
                "      _\\/\\\\\\_______\\/\\\\\\_____\\//\\\\\\\\\\\\//\\\\\\\\\\______/\\\\\\______\\//\\\\\\__  \n" +
                "       _\\/\\\\\\_______\\/\\\\\\______\\//\\\\\\__\\//\\\\\\______\\///\\\\\\\\\\\\\\\\\\\\\\/___ \n" +
                "        _\\///________\\///________\\///____\\///_________\\///////////_____\n");
        System.out.println(" Distriduted System Programming : PDF Document Conversion in the Cloud");
        System.out.println(" By Maor Assayag & Refahel Shetrit \n");
        System.out.println(" Stage 1|    Local AWS App has been started \n");


        // Initialize mAWS object and get a random UUID
        LocalAppID = UUID.randomUUID().toString();
        mAWS myAWS = new mAWS(true);
        myAWS.initEC2();
        myAWS.initS3();
        String managerID;
        try {
            /**2. Start a Manager instance on EC2 (if its not already running) */
            String[] results = checkManager(myAWS);
            managerID = results[0];
            if (managerID != null) {
                // Promotion of running Manager
                System.out.println(" Stage 2|    Manager instance already running, Manager ID : " + managerID + "\n");
            } else {
                managerID = results[1];
                Boolean restartResult = false;
                if (managerID !=null)
                    restartResult = myAWS.restartEC2instance(managerID);
                if (restartResult){
                    // Promotion of rebooted Manager
                    System.out.println(" Stage 2|    Manager instance has been rebooted, Manager ID : " + managerID + "\n");
                } else{
                    managerID = startManager(myAWS);
                    // Promotion of new Manager
                    System.out.println(" Stage 2|    Manager instance has been started, Manager ID : " + managerID + "\n");
                }
            }


            /** 3. Upload the input file to S3 bucket APP_BUCKET_NAME in folder FILES_FOLDER_NAME*/
            //myAWS.mCreateFolderS3(Header.APP_BUCKET_NAME + LocalAppID, Header.INPUT_FOLDER_NAME);
            String uploadedFileURL = uploadFileToS3(myAWS, inputFileName, Header.INPUT_FOLDER_NAME);
            System.out.println(" Stage 3|    The input file has been uploaded to " + uploadedFileURL + "\n");


            /** 4. Send the uploaded file URL to the SQS queue*/
            myAWS.initSQS();
            send2SQS(myAWS, LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL);
            if (terminate){
                System.out.println(" Stage 4|    Terminate message has been sent to the SQS queue \n");
            }else{
                System.out.println(" Stage 4|    The file URL has been sent to the SQS queue \n");
            }

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

    public static String convertPDF(String operation, String pdfURL) {
        String outputLine = operation + ":" + " " + pdfURL + " ";
        try {
            // Load PDF from URL
            //PDDocument pddDocument = PDDocument.load(new URL(pdfURL));
            PDDocument pddDocument = PDDocument.load(new URL(pdfURL));
            if (!pddDocument.isEncrypted()) {
                //TODO : save the files in EC2 and get the URL from it
                if (operation.equals("ToText")){
                    //ToText - convert the first page of the PDF file to a text file.
                    PDFTextStripper textStripper = new PDFTextStripper();

                    // Extract the first page of the PDF
                    textStripper.setStartPage(1);
                    textStripper.setEndPage(1);
                    String firstPage = textStripper.getText(pddDocument);

                    // Create new file
                    File file = new File(TEXT_NAME + ".txt");
                    file.createNewFile();
                    try {
                        PrintWriter out = new PrintWriter(file, "UTF-8");
                        out.println(firstPage);
                        //TODO change to EC2 url
                        outputLine = outputLine + file.getAbsolutePath();
                        out.close();
                    }catch (Exception e){
                        e.printStackTrace();
                        outputLine = outputLine + e.getMessage();
                    }

                } else if (operation.equals("ToImage")){
                    //ToImage - convert the first page of the PDF file to a "png" image.
                    PDFImageWriter writer = new PDFImageWriter();
                    writer.writeImage(pddDocument, IMAGE_FORMAT, null,1,1, IMAGE_NAME);
                    //TODO change to EC2 url
                    outputLine = outputLine + new java.io.File( "." ).getCanonicalPath() + File.separator + IMAGE_NAME+1+"."+IMAGE_FORMAT;

                }else if (operation.equals("ToHTML")){
                    //ToHTML - convert the first page of the PDF file to an HTML file.
                    PDFText2HTML pdfText2HTML = new PDFText2HTML(ENCODING);
                    pdfText2HTML.setStartPage(1);
                    pdfText2HTML.setEndPage(1);
                    FileWriter fWriter = null;
                    BufferedWriter bufferedWriter = null;

                    try {
                        fWriter = new FileWriter(HTML_NAME + ".html");
                        bufferedWriter = new BufferedWriter(fWriter);
                        pdfText2HTML.writeText(pddDocument,bufferedWriter);
                        bufferedWriter.close();
                        //TODO change to EC2 url
                        outputLine = outputLine + new java.io.File( "." ).getCanonicalPath() + File.separator + HTML_NAME + ".html";
                    } catch (Exception e) {
                        e.printStackTrace();
                        outputLine = outputLine + e.getMessage();
                    }

                }else{
                    outputLine = outputLine + "Unsupported operation: " + operation;

                }
            }else{
                outputLine = outputLine + "File is Encrypted";
            }

            pddDocument.close();
        } catch (Exception e) {
            //e.printStackTrace();
            outputLine = outputLine + e.getMessage();
        }
        return outputLine;
    }

    public static String analyzeTextFile(String inputFileURL){
        String outputURL = null;
        java.util.logging.Logger
                .getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.SEVERE);

        try {
            URL inputFileUrlObject = new URL(inputFileURL);
            Scanner s = new Scanner(inputFileUrlObject.openStream());

            // Create new file
            File file = new File(RESULT_NAME + ".txt");
            file.createNewFile();

            try {
                PrintWriter out = new PrintWriter(file, "UTF-8");
                String inputLine;
                String outputLine;
                while (s.hasNext()) {
                    inputLine = s.nextLine();

                    // Extract operation & URL
                    String[] parts = inputLine.split("\t"); // String array, each element is text between a tab
                    String operation = parts[0];
                    String currentPDFurl = null;
                    if (parts.length > 1) {
                        currentPDFurl = parts[1];
                    }

                    // Apply operation
                    outputLine = convertPDF(operation, currentPDFurl);

                    // Add the output line to the result text file
                    out.println(outputLine);
                }
                s.close();
                outputURL = file.getAbsolutePath();
                out.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return outputURL;
    }

    public static void testText2HTML(String outputFileName){
        try{
            PrintWriter out = new PrintWriter(outputFileName + ".HTML", "UTF-8");
            FileReader fileReader = new FileReader("C:\\Users\\MaorA\\IdeaProjects\\DSP\\pdf.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            out.println("<html>\n<body>");
            while ((line = bufferedReader.readLine()) != null)
                out.println(line + "<br>");

            int a  = 1;

            bufferedReader.close();

            out.println("</body>\n</html>");
            out.close();
            String outputFilePath = new java.io.File(".").getCanonicalPath() + File.separator + outputFileName + ".html";
            System.out.println(outputFilePath);
            }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * ----------------------------------------------------------
     */

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
    private static String startManager(mAWS myAWS) {
        uploadScripts(myAWS);
        uploadJars(myAWS);
        ArrayList<String> managerInstance = myAWS.initEC2instance("ami-0080e4c5bc078760e",
                1, 1, InstanceType.T2Micro.toString(), Header.APP_BUCKET_NAME + LocalAppID,
                Header.INPUT_FOLDER_NAME + "/" + Header.MANAGER_SCRIPT, Header.INSTANCE_KEY_NAME, TAG_MANAGER);
        return 	managerInstance.get(0);
    }

    /**
     * checkManager - method used to determined if the local app needs to start a new Manager
     * instance in aws.
     * @return instanceID if manager found, else null
     */
    private static String[] checkManager(mAWS aws) {
        String[] results = new String[2];
        results[0] = aws.getEC2instanceID(TAG_MANAGER, "running");
        results[1] = aws.getEC2instanceID(TAG_MANAGER, "stopped");
        return results;
    }

    /**
     * uploadFileToS3 - method that uploads the input file from the user to be read, distributed &
     * executed by a running manager.
     * @param inputFileName location of the file to upload to S3
     * @param aws amazon web service object with EC2 & S3
     * @return path (url) of the uploaded file in S3 - a confirmation of successful upload
     */
    private static String uploadFileToS3(mAWS aws, String inputFileName, String folder) {
        return aws.mUploadS3(Header.APP_BUCKET_NAME + LocalAppID, folder, LocalAppID, new File(inputFileName));
    }

    /**
     * send2SQS - method that send a message to Amazon Simple Queue Service :
     * A Fully managed message queues for microservices, distributed systems, and serverless applications.
     * The method initialize a Queue of messages (if needed) and send a message with the input file's URL and
     * information about the number of workers, LocalAppID etc.
     *
     * @param myAWS amazon web service object with EC2 & S3
     * @param msg the message to be sent
     */
    private static void send2SQS(mAWS myAWS, String msg) {
        String queueURL = myAWS.initSQSqueues(Header.INPUT_QUEUE_NAME, "0");
        myAWS.sendSQSmessage(queueURL, msg);
    }

    /**
     * waitForAnswer - method thats check the SQS in the cloud until there's a message
     * associate with the LocalAppID (meaning the Manager has responded and finished/stop
     * processing the requested operation)
     *  Blocking-IO method that sleeps for 'sleep' ms
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
            // busy-wait
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
        S3Object resultFile = aws.mDownloadS3file(Header.APP_BUCKET_NAME + LocalAppID, LocalAppID);
        InputStream inputStream  = resultFile.getObjectContent();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String outputFilePath = null;
        String line;

        try {
            PrintWriter out = new PrintWriter(outputFileName + ".HTML", "UTF-8");
            out.println("<html>\n<body>");

            while ((line = bufferedReader.readLine()) != null)
                out.println(line + "<br>");
            bufferedReader.close();

            out.println("</body>\n</html>");
            outputFilePath = new java.io.File(".").getCanonicalPath() + File.separator + outputFileName + ".html";
            out.close();

        } catch (Exception ex){
            ex.printStackTrace();
        }

        aws.mDeleteS3file(Header.APP_BUCKET_NAME + LocalAppID, LocalAppID);
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

    private static void uploadScripts(mAWS myAWS) {
        File managerScriptFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\src\\scriptManager.txt");
        String path = myAWS.mUploadS3(Header.APP_BUCKET_NAME + LocalAppID, Header.INPUT_FOLDER_NAME, Header.MANAGER_SCRIPT, managerScriptFile);
        System.out.println(" Stage 2|    Manager script has been uploaded to " + path + "\n");
//        File workerScriptFile = new File("scriptWorker.txt");
//        myAWS.mUploadS3(Header.APP_BUCKET_NAME, Header.WORKER_SCRIPT, workerScriptFile);
//        System.out.println("Worker Script Uploaded");
    }

    private static void uploadJars(mAWS myAWS) {
//        File localFile = new File("localapp.jar");
//        myAWS.mUploadS3(Header.APP_BUCKET_NAME, "localapp.jar", localFile);
//        System.out.println("LocalApplication Jar Uploaded");

        File managerFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\out\\artifacts\\ManagerApp_jar\\ManagerApp.jar");
        String path = myAWS.mUploadS3(Header.APP_BUCKET_NAME + LocalAppID, Header.INPUT_FOLDER_NAME,"ManagerApp.jar", managerFile);
        System.out.println(" Stage 3|    Manager jar has been uploaded to " + path + "\n");

//        File workerFile = new File("workerapp.jar");
//        myAWS.mUploadS3(Header.APP_BUCKET_NAME, "workerapp.jar", workerFile);
//        System.out.println("Worker Jar Uploaded");
        System.out.println("             Please make sure that public Bucket permission has been enabled on S3 aws console\n");
    }
}

