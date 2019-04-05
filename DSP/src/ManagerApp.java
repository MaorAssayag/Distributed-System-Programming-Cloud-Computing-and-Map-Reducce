import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.*;

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

    private static HashMap<String, String> myAWSsqsURL;

    public static void main(String[] args) throws IOException {

        System.out.println(" Stage 1|    Manager App has been started on EC2 instance\n");
        mAWS myAWS = new mAWS(false);
        myAWS.initAWSservices();

        try {
            List<Message> messages;
            String[] parsedMessage;
            String resultURL;
            initializeAllQueues(myAWS);
            System.out.println(" Stage 2|    Start listening to the following queue : " + Header.OUTPUT_QUEUE_NAME + "\n");

            while(true) {
                messages = myAWS.receiveSQSmessage(myAWSsqsURL.get(Header.INPUT_QUEUE_NAME)); // Receive List of all messages in queue
                for (Message message : messages) {
                    /**
                     * msg = LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL
                     * parsedMessage[0] = localAppID - first 12 is shortLocalAppID
                     * parsedMessage[1] = terminate - true/false
                     * parsedMessage[2] = n - number of workers
                     * parsedMessage[3] = uploadedFileURL - input file URL in S3
                     */
                    parsedMessage = message.getBody().split(" ");
                    System.out.println(" Stage 3|    Parsing the following message : \n");
                    System.out.println("             " + message.getBody() + "\n");

                    System.out.println(" Stage 4|    Analyzing the following input file : " + parsedMessage[3] + "\n");
                    resultURL = analyzeTextFile(myAWS, parsedMessage[0].substring(0,12), parsedMessage[3]);

                    System.out.println("\n Stage 5|    Computing complete, Sending the following message to the output queue : \n");
                    System.out.println("               " + parsedMessage[0].substring(0,12) + " " + resultURL + "\n");
                    myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), parsedMessage[0].substring(0,12) + " " + resultURL);
                    //TODO add printing all over the place
                    //TODO : check locally and in S3 for every file that everything is been ok
                    //TODO : generate jar files, dont forget to change credintials boolean
                    //TODO : check on cloud manager if the localAPP got response and ended
                    //TODO : workers?
                    String myMessage = message.getReceiptHandle();
                    myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), myMessage); // Delete the message from the queue
                    System.out.println(" Stage 6|    Busy-wait to new messages..." + "\n");
                }
                // busy-wait
                try {Thread.sleep(2000);}
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
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_QUEUE_NAME, "0"));
        myAWSsqsURL = myAWS.initSQSqueues(queues);
    }

    public static String analyzeTextFile(mAWS myAWS, String shortLocalAppID, String inputFileURL){
        String outputURL = null;
        java.util.logging.Logger
                .getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.SEVERE);

        try {
            URI fileToBeDownloaded = new URI(inputFileURL);
            AmazonS3URI s3URI = new AmazonS3URI(fileToBeDownloaded);
            S3Object inputFile = myAWS.mDownloadS3file(s3URI.getBucket(), s3URI.getKey());

            InputStream inputStream  = inputFile.getObjectContent();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            //URL inputFileUrlObject = new URL(inputFileURL);
            //Scanner s = new Scanner(inputFileUrlObject.openStream());

            // Create new file
            File file = new File(Header.RESULT_FILE_NAME);
            file.createNewFile();
            System.out.println("             Fetching files & computing : ");
            try {
                PrintWriter out = new PrintWriter(file, "UTF-8");
                String inputLine;
                String outputLine;
                int index = 0;
                while ((inputLine = bufferedReader.readLine()) != null){
                    System.out.print(".");
                    //while (s.hasNext()) {
                   // inputLine = s.nextLine();

                    // Extract operation & URL
                    String[] parts = inputLine.split("\t"); // String array, each element is text between a tab
                    String operation = parts[0];
                    String currentPDFurl = null;
                    if (parts.length > 1) {
                        currentPDFurl = parts[1];
                    }

                    // Apply operation
                    outputLine = convertPDF(myAWS, shortLocalAppID, operation, currentPDFurl, index);

                    // Add the output line to the result text file
                    out.println(outputLine);

                    index++;
                }
                bufferedReader.close();
                //outputURL = file.getAbsolutePath();
                out.close();

                // Upload File file to app_bucket+LocalID S3 and return the URL
                String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, null, Header.RESULT_FILE_NAME, file);
                outputURL = newURL;

            }catch (Exception e){
                e.printStackTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return outputURL;
    }

    public static String convertPDF(mAWS myAWS, String shortLocalAppID, String operation, String pdfURL, int index) {
        String outputLine = operation + ":" + "\t" + pdfURL + " ";
        try {
            // Load PDF from URL
            PDDocument pddDocument = PDDocument.load(new URL(pdfURL));
            if (!pddDocument.isEncrypted()) {
                if (operation.equals("ToText")){
                    //ToText - convert the first page of the PDF file to a text file.
                    PDFTextStripper textStripper = new PDFTextStripper();

                    // Extract the first page of the PDF
                    textStripper.setStartPage(1);
                    textStripper.setEndPage(1);
                    String firstPage = textStripper.getText(pddDocument);

                    // Create new file
                    File file = new File("temp" + ".txt");
                    file.createNewFile();
                    PrintWriter out = new PrintWriter(file, "UTF-8");
                    out.println(firstPage);

                    // Upload to EC2
                    String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, Header.OUTPUT_FOLDER_NAME, index+".txt", file);
                    outputLine = outputLine + newURL;
                    out.close();

                } else if (operation.equals("ToImage")){
                    //ToImage - convert the first page of the PDF file to a "png" image.
                    PDFImageWriter writer = new PDFImageWriter();
                    writer.writeImage(pddDocument, Header.IMAGE_FORMAT, null,1,1, Header.IMAGE_NAME);
                    //outputLine = outputLine + new java.io.File( "." ).getCanonicalPath() + File.separator + Header.IMAGE_NAME+index+"."+IMAGE_FORMAT;

                    // Upload to EC2
                    // PDFImageWrite write the png to temp1.png constantly
                    File file = new File(new java.io.File( "." ).getCanonicalPath() + File.separator + Header.IMAGE_NAME+1+"."+Header.IMAGE_FORMAT);
                    String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, Header.OUTPUT_FOLDER_NAME, index+"."+Header.IMAGE_FORMAT, file);
                    outputLine = outputLine + newURL;

                }else if (operation.equals("ToHTML")){
                    //ToHTML - convert the first page of the PDF file to an HTML file.
                    PDFText2HTML pdfText2HTML = new PDFText2HTML(Header.ENCODING);
                    pdfText2HTML.setStartPage(1);
                    pdfText2HTML.setEndPage(1);
                    FileWriter fWriter = null;
                    BufferedWriter bufferedWriter = null;

                    // Create new file
                    fWriter = new FileWriter(Header.HTML_NAME + ".html");
                    bufferedWriter = new BufferedWriter(fWriter);
                    pdfText2HTML.writeText(pddDocument,bufferedWriter);
                    bufferedWriter.close();

                    // Upload to EC2
                    //outputLine = outputLine + new java.io.File( "." ).getCanonicalPath() + File.separator + HTML_NAME + ".html";
                    File file = new File(new java.io.File( "." ).getCanonicalPath() + File.separator + Header.HTML_NAME+".html");
                    String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, Header.OUTPUT_FOLDER_NAME, index+".html", file);
                    outputLine = outputLine + newURL;

                }else{
                    outputLine = outputLine + "Error: Unsupported operation: " + operation;
                }
            }else{
                outputLine = outputLine + "Error: File is Encrypted";
            }
            pddDocument.close();
        } catch (Exception e) {
            //e.printStackTrace();
            try{
                //outputLine = outputLine + "Error:" + e.getCause().getMessage();
                outputLine = outputLine + "Error: PDF file not found - " + e.getClass().getName();
            } catch (Exception ex){
                //outputLine = outputLine + "Error:" + e.getMessage();
                outputLine = outputLine + "Error: PDF file not found";
            }
        }
        return outputLine;
    }

}
