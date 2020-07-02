package examples.taskqueue;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import messaging.ExampleProto.MessageCount;
import messaging.ExampleProto.WordCountService;
import messaging.ExampleProto.MessageString;
import rpc.RpcChannel;
import rpc.RpcServer;
import taskqueue.Master;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import static java.util.Collections.sort;

/**
 * Word Counter example to demonstrate task-queue, a WordCountMaster that submit tasks in form
 * of Strings and a group of WordCountWorkers that process the tasks and count the words and
 * forward the results back to the WordCountMaster to come up with a total word sum
 *
 * @author paulcao
 *
 */
public class WordCountMaster {

    public static Random random = new Random();

    /**
     * Generate a random word
     *
     * @return a random word
     */
    public static String randomWord() {
        char[] word = new char[random.nextInt(8)+3]; // words of length 3 through 10. (1 and 2 letter words are boring.)

        for (int i = 0; i<word.length; i++)
        {
            word[i] = (char)('a' + random.nextInt(26));
        }

        return new String(word);
    }

    /**
     * Generate a sequence of random words
     *
     * @return sequence of random words
     */
    public static String randomWords() {
       // int numberOfWords = random.nextInt(26);
        int numberOfWords=2;
        String words = "";
        for (int i=0; i<numberOfWords; i++) {
            words += Integer.toString(random.nextInt(670)) + " ";
        }
        if(random.nextInt(2) == 0){
            words += "+ ";
        }else{
            words += "- ";
        }
        return words;
    }

    public static void main(String[] args) throws InterruptedException {

        // Set up a group of 10 WordCountWorker nodes

        Set<WordCountWorker> wordCountWorkers = new HashSet<WordCountWorker>();
        Map<String,ArrayList< Double>> measureTimeMap  = new HashMap<String, ArrayList<Double>>();

        for (int i=0;i<3;i++) {
            wordCountWorkers.add(new WordCountWorker("127.0.0.1", 5555, "127.0.0.1", 5556, 1, i,measureTimeMap));
        }

        // Wait for 0.5 seconds to sync up initially the WordCountWorker nodes binding to the WordCountMaster node
        // TO-DO: Have better synchronization amongst worker and master nodes instead of wait
        Thread.sleep(500);

        // Set up the WordCountMaster service scaffolding
        WordCountService wordCountService =  WordCountService.newStub(new Master("127.0.0.1", 5555,
                "127.0.0.1", 5556, 1));
        AtomicInteger wordCountTotalSum = new AtomicInteger(0);

        int numberOfRequests = 1001;
        // Submit 100 Strings for worker nodes to count
        for (int i=0;i<numberOfRequests;i++) {
            // Generate a sequence of random words
            MessageString document;
            if(i != numberOfRequests-1) {
                 document = MessageString.newBuilder().setMessage(randomWords() + " " + System.currentTimeMillis()).build();
            } else{
                document=MessageString.newBuilder().setMessage("0 0 + " + " " + System.currentTimeMillis()).build();
            }
           // System.out.println("Sending numbers: " + document);
            // Send word count task evenly distributed to worker nodes
            wordCountService.wordCount(null, document, new RpcCallback<MessageCount>() {
                @Override
                public void run(MessageCount msg) {
                    // Print the current grand word count sum of all documents/strings submitted thus far
                    int result = msg.getCount();
                   // System.out.println("Current result: " + result);
                }
            });
        }



        // Block the current thread as tasks submission and collections are asynchronous
        while (true) {}
    }
}