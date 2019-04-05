import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.Base64;

import java.io.*;

public class preUpload {

    public static void main(String[] args) {

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
        System.out.println(" Stage 1|    Pre-upload of scripts & jars has been started\n");

        mAWS myAWS= new mAWS(true);
        myAWS.initAWSservices();
        upload(myAWS);
        delete(myAWS);
        //System.out.println(getScript(""));


        System.out.println(" _______________   __________ \n" +
                " ___  ____/___  | / /___  __ \\\n" +
                " __  __/   __   |/ / __  / / /\n" +
                " _  /___   _  /|  /  _  /_/ / \n" +
                " /_____/   /_/ |_/   /_____/");
        System.out.println("****************************************************************\n");
    }

    private static void upload(mAWS myAWS) {
        //uploadScripts(myAWS);
        //uploadJars(myAWS);
    }

//    private static void uploadScripts(mAWS awsO) {
//        File managerScriptFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\src\\scriptManager.txt");
//        String path = awsO.mUploadS3(Header.APP_BUCKET_NAME, Header.PRE_FOLDER_NAME, Header.MANAGER_SCRIPT, managerScriptFile);
//        System.out.println(" Stage 2|    Manager script has been uploaded to " + path + "\n");
//
////        File workerScriptFile = new File("scriptWorker.txt");
////        awsO.mUploadS3(Header.APP_BUCKET_NAME, Header.WORKER_SCRIPT, workerScriptFile);
////        System.out.println("Worker Script Uploaded");
//    }

//    private static void uploadJars(mAWS myAWS) {
////        File localFile = new File("localapp.jar");
////        myAWS.mUploadS3(Header.APP_BUCKET_NAME, "localapp.jar", localFile);
////        System.out.println("LocalApplication Jar Uploaded");
//
//        File managerFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\out\\artifacts\\ManagerApp_jar\\ManagerApp.jar");
//        String path = myAWS.mUploadS3(Header.APP_BUCKET_NAME, Header.PRE_FOLDER_NAME,"ManagerApp.jar", managerFile);
//        System.out.println(" Stage 3|    Manager jar has been uploaded to " + path + "\n");
//
////        File workerFile = new File("workerapp.jar");
////        myAWS.mUploadS3(Header.APP_BUCKET_NAME, "workerapp.jar", workerFile);
////        System.out.println("Worker Jar Uploaded");
//
//        System.out.println("             Please make sure that public Bucket permission has been enabled on S3 aws console\n");
//    }

    private static void delete(mAWS awsO) {
        //deleteS3Buckets(awsO);

        try {
            awsO.deleteSQSqueueMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(" Stage 4|    Queues has been deleted\n");

        awsO.terminateEC2all();
        System.out.println(" Stage 5|    Ec2 instances has been deleted\n");
    }

    private static void deleteS3Buckets(mAWS awsO) {
//        try {
//            awsO.mDeleteS3bucket(Header.INPUT_BUCKET_NAME);
//            System.out.println("Input bucket deleted");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        try{
//            awsO.mDeleteS3bucketFiles(Header.OUTPUT_BUCKET_NAME);
//            System.out.println("Output bucket deleted");
//        } catch (Exception e){
//            e.printStackTrace();
//        }
    }

    private static String getScript(String userData) {
        //Download script from S3
        //S3Object object = mDownloadS3file(Header.APP_BUCKET_NAME, userData);
        //InputStream input = object.getObjectContent();
        String ans = null;
        try {
            String script = null;
            FileReader fileReader = new FileReader("C:\\Users\\MaorA\\IdeaProjects\\DSP\\src\\scriptManager.txt");
            BufferedReader reader = new BufferedReader(fileReader);
            try {
                StringBuilder stringBuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(line);
                    stringBuilder.append("\n");
                }
                script = stringBuilder.toString();
                reader.close();
            } catch (Exception exception) {
                exception.printStackTrace();
            }

            try {
                System.out.println(script);
                //ans = new String(Base64.encode(script.getBytes()));
                ans = Base64.encodeAsString(script.getBytes());
                //ans = new String( Base64.encode( script.getBytes( "UTF-8" )), "UTF-8" );
            } catch (NullPointerException npe) {
                npe.printStackTrace();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return ans;
    }

}