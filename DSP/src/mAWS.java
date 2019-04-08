import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.util.Base64;

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
 * mAWS class - Barebone of AWS handling. Creating, Managing & Terminating instance of
 * S3 storage, EC2 instances and SQS queue of messages.
 */
public class mAWS {

    private AmazonSQS mSQS;
    private AmazonEC2 mEC2;
    private AmazonS3 mS3;
    private AWSCredentials credentials;
    private boolean fromLocal;

    /**
     * Get your credentials from the "credentials" file inside you .aws folder
     */
    public mAWS(boolean fromLocal){
        this.fromLocal = fromLocal;
        if(fromLocal){
            credentials = new ProfileCredentialsProvider().getCredentials();
        }
    }

    /**
     * Get your credentials by hard-coded code
     */
    public mAWS(String accessKey, String secretKey) {
        credentials = new BasicAWSCredentials(accessKey, secretKey);
    }

    public AWSCredentials getCredentials(){
        return credentials;
    }

    public void initAWSservices(){
        initEC2();
        initS3();
        initSQS();
    }

    /**
     * initialize EC2 service
     */
    public void initEC2(){
        if (this.fromLocal){
            mEC2 = AmazonEC2ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else{
            mEC2 = AmazonEC2ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }
    }

    /**
     *
     * @param tag to identify the instance we are looking for
     * @param state of the instance (ex. running, stopped, pending etc)
     * @return id of instance found by Tag and state
     */
    public String getEC2instanceID(Tag tag, String state){

        List<Reservation> reservations = mEC2.describeInstances().getReservations();

        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                for (Tag instanceTag : instance.getTags()) {
                    if(instanceTag.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
                        // e.g.  the instance Tag name=Type and the value=Manager
                        if(instance.getState().getName().equals(state)) {
                            //System.out.println(instance.getInstanceId());
                            return instance.getInstanceId();
                        }
                    }
                }
            }
        }
        return null;
    }

    public int getEC2instancesByTagState(Tag tag, String state){
        List<Reservation> reservations = mEC2.describeInstances().getReservations();
        int ans = 0;
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                for (Tag instanceTag : instance.getTags()) {
                    if(instanceTag.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
                        // e.g.  the instance Tag name=Type and the value=Manager
                        if(instance.getState().getName().equals(state)) {
                            //System.out.println(instance.getInstanceId());
                            ans++;
                        }
                    }
                }
            }
        }
        return ans;
    }


    /**
     *
     * @param imageId (for ex. "ami-b66ed3de")
     * @param minCount
     * @param maxCount
     * @param type (ex. T2Small)
     * @param userData (txt file containing the script for the instance to run when started)
     * @param keyName (name for the new instance)
     * @param tag
     * @return list with all the instance's id created
     */
    public ArrayList<String> initEC2instance(String imageId, Integer minCount, Integer maxCount, String type, String bucketName, String userData, String keyName, Tag tag){

        ArrayList<String> instancesId = new ArrayList<String>();
        String userScript = null;

        try {
            userScript = getScript(bucketName, userData);

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("\"             Starting an instance without a script \n");
        }

        RunInstancesRequest request = new RunInstancesRequest(imageId, minCount, maxCount);
        request.setInstanceType(type);
        request.withKeyName(keyName);
        request.withAdditionalInfo("worker");
        request.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(keyName));
                //new IamInstanceProfileSpecification().withArn("arn:aws:iam::951925995010:instance-profile/managerInstance"));
        if (userScript != null)
            request.withUserData(userScript);

        List<Instance> instances = mEC2.runInstances(request).getReservation().getInstances();
        List<Tag> tags = new ArrayList<Tag>();
        tags.add(tag);
        CreateTagsRequest tagsRequest = new CreateTagsRequest();
        tagsRequest.setTags(tags);
        String instanceID;

        // Create tag request for each instance (if we want to denote 20 workers then we want tags for them)
        for (Instance instance : instances) {
            try {
                if (instance.getState().getName().equals("pending") || instance.getState().getName().equals("running")){
                    instanceID = instance.getInstanceId();
                    tagsRequest.withResources(instanceID);
                    mEC2.createTags(tagsRequest);
                    instancesId.add(instanceID);
                }
            } catch (Exception e) {
                System.out.println("             Error Message on initEC2instance instances tag request : " + e.getMessage());
            }
        }
        return instancesId;
    }

    /**
     * restartEC2instance
     * Used to restart already existing (but stopped) instances
     * @param instanceID - the stopped instance
     * @return if the stopped instance has been restart successfully
     */
    public Boolean restartEC2instance(String instanceID){
        try{
            StartInstancesRequest request = new StartInstancesRequest();
            request.withInstanceIds(instanceID);
            StartInstancesResult result = mEC2.startInstances(request);
            List<InstanceStateChange> instancesStates = result.getStartingInstances();
            for (InstanceStateChange instanceState : instancesStates){
                if (instanceState.getInstanceId().equals(instanceID)){
                    return instanceState.getCurrentState().getName().equals("running") || instanceState.getCurrentState().getName().equals("pending");
                }
            }
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @param instanceId to terminate
     */
    public void terminateEC2instance(String instanceId){
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceId);
        mEC2.terminateInstances(request);
    }

    /** @param instanceId to terminate
     */
    public void terminateEC2instance(Collection<String> instanceId){
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceId);
        mEC2.terminateInstances(request);
    }

    public void terminateEC2all(){
        DescribeInstancesResult describeInstancesRequest = mEC2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();

        Set<Instance> instances = new HashSet<Instance>();
        for (Reservation reservation : reservations) {
            instances.addAll(reservation.getInstances());
        }

        ArrayList<String> instancesId = new ArrayList<String>();
        for (Instance ins : instances){
            instancesId.add(ins.getInstanceId());
        }

        terminateEC2instance(instancesId);
    }

    /**
     * initialize S3 services
     */
    public void initS3(){
        if(this.fromLocal){
            mS3 = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else{
            mS3 = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }
    }

    /**
     * @return S3 url of the uploaded file
     */
    public String mUploadS3(String bucketName, String folderName, String key, File file){
        if (folderName != null){
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, folderName + "/" + key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + folderName + "/" + key; // return the url of the uploaded file
        } else{
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + key; // return the url of the uploaded file
        }
    }

    public void mCreateFolderS3(String bucketName, String folderName) {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);


        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                folderName + "/", emptyContent, metadata);

        // send request to S3 to create folder
        mS3.putObject(putObjectRequest);
    }

    /**
     * @return object at the bucket and with the key specified
     */
    public S3Object mDownloadS3file(String bucketName, String key){
        return mS3.getObject(new GetObjectRequest(bucketName, key));
    }

    public boolean doesFileExist(String bucketName, String key){
        return mS3.doesObjectExist(bucketName, key);
    }

    /**
     * Delete object from the bucket
     */
    public void mDeleteS3file(String bucketName, String key){
        mS3.deleteObject(new DeleteObjectRequest(bucketName, key));
    }

    /**
     * A bucket must be completely empty before it can be deleted
     * @param bucketName to delete
     */
    public void mDeleteS3bucket(String bucketName) {
        mS3.deleteBucket(bucketName);
    }

    /**
     * Deletes a bucket and all the files inside
     */
    public void mDeleteS3bucketFiles(String bucketName){
        ObjectListing objectListing = mS3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            mDeleteS3file(bucketName, objectSummary.getKey());
        }
        mDeleteS3bucket(bucketName);
    }

    /**
     * initialize SQS services
     */
    public void initSQS(){
        if(this.fromLocal){
            mSQS = AmazonSQSClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else{
            mSQS = AmazonSQSClientBuilder
                    .standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }
    }

    /**
     * Initialize a list of queues
     * @param queues list of queues' names and their visibility timeout to be initialized
     * @return list of each queue's URL
     */
    public HashMap<String, String> initSQSqueues(ArrayList<Entry<String, String>> queues){
        HashMap<String, String> queuesURLs = new HashMap<String, String>();
        String queueURL = null;

        for (Entry<String, String> pair : queues) {
            String queueName = pair.getKey();
            try {
                queueURL = mSQS.getQueueUrl(queueName).getQueueUrl();
            }
            catch(AmazonServiceException exception) {
                if (exception.getStatusCode() == 400) { // not found
                    CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
                    Map<String, String> attributes = new HashMap<String, String>();
                    attributes.put("VisibilityTimeout", pair.getValue());
                    createQueueRequest.setAttributes(attributes);
                    queueURL = mSQS.createQueue(createQueueRequest).getQueueUrl();
                    System.out.println("             The following queue has been created : " + queueURL + "\n");
                }
                else {
                    System.out.println("Caught Exception: " + exception.getMessage());
                    System.out.println("Reponse Status Code: " + exception.getStatusCode());
                    System.out.println("Error Code: " + exception.getErrorCode());
                    System.out.println("Request ID: " + exception.getRequestId());
                }
            } catch (Exception exception){
                exception.printStackTrace();
            }
            queuesURLs.put(queueName, queueURL);
        }
        return queuesURLs;
    }

    /**
     * Initialize only one queue
     * @param queueName to initialize
     * @return URL of the queue
     */
    public String initSQSqueues(String queueName, String visibilityTimeout){
        ArrayList<Entry<String, String>> queue = new ArrayList<Entry<String, String>>();
        queue.add(new SimpleEntry<String, String>(queueName, visibilityTimeout));
        return initSQSqueues(queue).get(queueName);
    }

    /**
     *
     * @param queueURL URL of the queue to which we want to send the message
     * @param message the message to be sent
     */
    public void sendSQSmessage(String queueURL, String message){
        mSQS.sendMessage(new SendMessageRequest(queueURL, message));
    }

    /**
     *
     * @param queueURL URL of the queue we want to pull messages from
     * @return list of all the messages in the queue
     */
    public List<Message> receiveSQSmessage(String queueURL){
        // Create request to retrieve a list of messages in the SQS queue
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueURL);
        return mSQS.receiveMessage(request).getMessages();
    }

    /**
     *
     * @param request get messages with a personalized request
     * @return list of all messages received
     */
    public List<Message> receiveSQSmessage(ReceiveMessageRequest request){
        return mSQS.receiveMessage(request).getMessages();
    }

    /**
     * @param queueUrl from which to delete
     * @param receiptHandle of the message to delete
     */
    public void deleteSQSmessage(String queueUrl, String receiptHandle) {
        mSQS.deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle));
    }

    /** @param queueUrl to delete
     */
    public void deleteSQSqueue(String queueUrl) {
        mSQS.deleteQueue(new DeleteQueueRequest(queueUrl));
    }

    /**
     * Delete all the Queues in SQS
     */
    public void deleteSQSqueueMessages(){
        for (String queueUrl : mSQS.listQueues().getQueueUrls()) {
            deleteSQSqueue(queueUrl);
        }
    }

    /**
     *
     * @param userData file containing the script
     * @return script encoded in base64
     */
    private String getScript(String bucketName, String userData) {
        //Download script from S3
        S3Object object = mDownloadS3file(bucketName, userData);
        InputStream input = object.getObjectContent();

        String script = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        try {
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null){
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            script = stringBuilder.toString();
            reader.close();
        } catch (Exception exception){
            exception.printStackTrace();
        }

        String ans = null;
        try{
            ans = new String(Base64.encode(script.getBytes()));
        }catch (NullPointerException npe){
            npe.printStackTrace();
        }
        return ans;
    }
}