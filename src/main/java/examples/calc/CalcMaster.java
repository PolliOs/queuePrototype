package examples.calc;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import taskqueue.Master;

import com.google.protobuf.RpcCallback;
import messaging.QueueCalc.AddService;
import messaging.QueueCalc.SubtractionService;
import messaging.QueueCalc.RequestMessage;
import messaging.QueueCalc.MessageResult;

import static java.util.Collections.sort;

/**
 * Word Counter example to demonstrate task-queue, a WordCountMaster that submit tasks in form
 * of Strings and a group of WordCountWorkers that process the tasks and count the words and
 * forward the results back to the WordCountMaster to come up with a total word sum
 *
 * @author paulcao
 *
 */
public class CalcMaster {

    public static Random random = new Random();


    public static void main(String[] args) throws InterruptedException {

        Set<CalcWorker> calcWorkers = new HashSet<CalcWorker>();
        Map<String,ArrayList< Double>> measureTimeMap  = new HashMap<String, ArrayList<Double>>();

        for (int i=0;i<3;i++) {
            calcWorkers.add(new CalcWorker("127.0.0.1", 5555, "127.0.0.1", 5556, 1, i,measureTimeMap));
        }

        // Wait for 0.5 seconds to sync up initially the CalcWorker nodes binding to the CalcMaster node
        // TO-DO: Have better synchronization amongst worker and master nodes instead of wait
        Thread.sleep(500);

        // Set up the CalcMaster AddService scaffolding
        AddService addService =  AddService.newStub(new Master("127.0.0.1", 5555,
                "127.0.0.1", 5556, 1));
        SubtractionService subtractionServiceService =  SubtractionService.newStub(new Master("127.0.0.1", 5555,
                "127.0.0.1", 5556, 1));

        int numberOfRequests = 1001;
        for (int i=0;i<numberOfRequests;i++) {
            RequestMessage requestMessage;
            if(i != numberOfRequests-1) {
                requestMessage = RequestMessage.newBuilder().setNumberA(random.nextInt(670)).setNumberB(random.nextInt(670)).setStartTime(System.currentTimeMillis()).build();
            } else{
                requestMessage = RequestMessage.newBuilder().setNumberA(0).setNumberB(0).setStartTime(System.currentTimeMillis()).build();
            }
            addService.add(null, requestMessage, new RpcCallback<MessageResult>() {
                @Override
                public void run(MessageResult msg) {
                    int result = msg.getMessage();
                    // System.out.println("Current result: " + result);
                }
            });
        }



        // Block the current thread as tasks submission and collections are asynchronous
        while (true) {}
    }
}