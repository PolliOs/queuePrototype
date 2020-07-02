package examples.calc;

import messaging.QueueCalc.CalcService;
import messaging.QueueCalc.RequestMessage;
import taskqueue.Master;

import java.util.*;

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

        Set<CalcWorker> calcWorkers = new HashSet<>();
        Map<String,ArrayList< Double>> measureTimeMap  = new HashMap<>();

        for (int i=0;i<3;i++) {
            calcWorkers.add(new CalcWorker("127.0.0.1", 5555, "127.0.0.1", 5556+i, 1, i,measureTimeMap));
        }

        // Wait for 0.5 seconds to sync up initially the CalcWorker nodes binding to the CalcMaster node
        // TO-DO: Have better synchronization amongst worker and master nodes instead of wait
        Thread.sleep(500);

        // Set up the CalcMaster CalcService scaffolding
        CalcService calcService =  CalcService.newStub(new Master("127.0.0.1", 5555,
                "127.0.0.1", 5556, 1));

        int numberOfRequests = 1001;
        for (int i=0;i<numberOfRequests;i++) {
            RequestMessage requestMessage;
            if(i != numberOfRequests-1) {
                requestMessage = RequestMessage.newBuilder().setNumberA(random.nextInt(670)).setNumberB(random.nextInt(670)).setStartTime(System.currentTimeMillis()).build();
            } else{
                requestMessage = RequestMessage.newBuilder().setNumberA(0).setNumberB(0).setStartTime(System.currentTimeMillis()).build();
            }
            if(i%2==0) {
                calcService.add(null, requestMessage, msg -> {});
            }else{
                calcService.subtract(null, requestMessage, msg -> {});

            }
        }



        // Block the current thread as tasks submission and collections are asynchronous
        while (true) {}
    }
}