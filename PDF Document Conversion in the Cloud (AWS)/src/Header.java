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
 * Header class - Global (static) final values across all classes
 */

public class Header {
    // Key name of the .pem file that contains identifiers to access the instances
    static final String INSTANCE_MANAGER_KEY_NAME = "managerInstance";
    static final String INSTANCE_WORKER_KEY_NAME = "managerInstance";
    static final String imageID = "ami-0080e4c5bc078760e";

    // Bucket with all the jars and scripts
    static final String PRE_UPLOAD_BUCKET_NAME = "mr-dsp-192-pre-upload-jars-scripts";

    // General Bucket for new localApps
    static final String APP_BUCKET_NAME = "dsp-192-local-app-";

    // Folders
    static final String INPUT_FOLDER_NAME = "input-folder";
    static final String OUTPUT_FOLDER_NAME = "output-folder";

    // Files
    static final String INPUT_FILE_NAME = "input-file.txt";
    static final String RESULT_FILE_NAME = "result-file.txt";
    static final String LOGGER_FILE_NAME = "ManagerLogger.txt";
    static final String IMAGE_FORMAT = "png";
    static final String IMAGE_NAME = "temp";
    static final String TEXT_NAME = "temp";
    static final String HTML_NAME = "tmep";
    static final String ENCODING = "UTF-8";

    // Strings for SQS messages
    static final String TERMINATED_STRING = "TERMINATED"; //Tells the localApp it can terminate the Manager Instance

    // Scripts
    static final String MANAGER_SCRIPT = "scriptManager.txt";
    static final String WORKER_SCRIPT = "scriptWorker.txt";

    // Jars
    static final String MANAGER_JAR = "ManagerApp.jar";
    static final String WORKER_JAR = "Worker.jar";

    // Queues Names
    static final String INPUT_QUEUE_NAME = "inputQueue";
    static final String INPUT_THREAD_QUEUE_NAME = "inputThreadsQueue";
    static final String INPUT_WORKERS_QUEUE_NAME = "inputWorkerQueue";
    static final String OUTPUT_WORKERS_QUEUE_NAME = "outputWorkerQueue";
    static final String OUTPUT_QUEUE_NAME = "outputQueue";

    // global waiting time
    static final int sleep = 2000;
    static final int sleepFetchingFromWorkers = 250;
    static final int localAppWaiting = 2000;
}
