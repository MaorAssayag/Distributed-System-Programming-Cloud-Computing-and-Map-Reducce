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
 * Header class - Global (static) final values across all classes
 */

public class Header {
    // Key name of the .pem file that contains identifiers to access the instances
    static final String INSTANCE_KEY_NAME = "managerInstance";

    // Bucket with all the jars and scripts
    static final String APP_BUCKET_NAME = "app-bucket-dsp-192-maor-refahel";

    // Buckets name
    static final String INPUT_BUCKET_NAME = "input-bucket-dsp-192-maor-refahel";
    static final String OUTPUT_BUCKET_NAME = "output-bucket-dsp-192-maor-refahel";

    // Strings for SQS messages
    static final String MESSAGE_DIFF = "#s";
    static final String TERMINATED_STRING = "TERMINATED"; //Tells the localApp it can terminate the Manager Instance

    // Scripts
    static final String MANAGER_SCRIPT = "scriptManager.txt";
    static final String WORKER_SCRIPT = "scriptWorker.txt";

    // Queues Names
    static final String INPUT_QUEUE_NAME = "inputQueue";
    static final String OUTPUT_QUEUE_NAME = "outputQueue";

}
