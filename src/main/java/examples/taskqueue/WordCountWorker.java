package examples.taskqueue;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import messaging.ExampleProto.MessageCount;
import messaging.ExampleProto.MessageString;
import messaging.ExampleProto.WordCountService;
import taskqueue.Worker;

import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.sort;

/**
 * WordCountWorker is a implementation of a worker node that receives Strings
 * and tally up the number of words contained and return back the count
 *
 * @author paulcao
 *
 */
public class WordCountWorker {

    /**
     * Constructor
     * @param host master hostname
     * @param port master port
     * @param senderHost master task sink hostname
     * @param senderPort master task sink port
     * @param io_threads threads dedicated to the zmq socket
     * @param nodeId id of the worker node
     * @param measureTimeMap add the time metrics for measuring
     */
    public WordCountWorker(String host, int port,
                           String senderHost, int senderPort, int io_threads, int nodeId, Map<String, ArrayList<Double>> measureTimeMap){
        // initialize the worker node and implement the WordCountService scaffold
        Worker worker = new Worker("127.0.0.1", 5555, "127.0.0.1", 5556, 1);
        worker.registerService(new WordCountService() {
            @Override
            public void wordCount(RpcController controller,
                                  MessageString request, RpcCallback<MessageCount> done) {

                // count the words from the request String
                long receivedRequestTime = System.currentTimeMillis();
                String[] wordArray = request.getMessage().split("\\s+");
                int result = 0;
                for(int i = 0; i < 2; i++){
                    result += Integer.valueOf(wordArray[i]);
                }
                long sentRequestTime = Long.valueOf(wordArray[2]);
                MessageCount count = MessageCount.newBuilder().setCount(result).build();
               // System.out.println("Work performed by node " + nodeId);
                //System.out.println("QueueWaitTime: " + (receivedRequestTime-sentRequestTime));
                if(result %10 > 5){
                    try {
                        Thread.sleep(500);
                        //System.out.println("worker# " + nodeId + "is sleeping");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if(result==0){
                    showResults();
                }

                if (done != null) {
                    done.run(count);	// forward the count to the call-back handler and master task result collector

                    long sentReplyTime = System.currentTimeMillis();
                    addMetrics(measureTimeMap,sentRequestTime,sentReplyTime,receivedRequestTime);
                    //System.out.println("MessageProcessingTime: " + (sentReplyTime - sentRequestTime));

                }
            }

            private void addMetrics(Map<String, ArrayList<Double>> measureTimeMap, long sentRequestTime, long sentReplyTime, long receivedRequestTime) {
                if(!measureTimeMap.containsKey("WorkerProcessingTime")){
                    ArrayList<Double> workerProcessing = new ArrayList<>();
                    workerProcessing.add((double)(sentReplyTime - receivedRequestTime));
                    measureTimeMap.put("WorkerProcessingTime",workerProcessing);
                }else{
                    measureTimeMap.get("WorkerProcessingTime").add((double)(sentReplyTime - receivedRequestTime));
                }
                // System.out.println("WorkerProcessingTime: " + (sentReplyTime - receivedRequestTime));
                if(!measureTimeMap.containsKey("MessageProcessingTime")){
                    ArrayList<Double> messageProcessing = new ArrayList<>();
                    messageProcessing.add((double)sentReplyTime - sentRequestTime);
                    measureTimeMap.put("MessageProcessingTime",messageProcessing);
                }else{
                    measureTimeMap.get("MessageProcessingTime").add((double)sentReplyTime-sentRequestTime);
                }

                if(!measureTimeMap.containsKey("QueueWaitTime")){
                    ArrayList<Double> timeDiff = new ArrayList<>();
                    timeDiff.add((double)receivedRequestTime-sentRequestTime);
                    measureTimeMap.put("QueueWaitTime",timeDiff);
                }else{
                    measureTimeMap.get("QueueWaitTime").add((double)receivedRequestTime-sentRequestTime);
                }
            }

            private void showResults() {
                    for (Map.Entry<String, ArrayList<Double>> timeMetric:
                            measureTimeMap.entrySet()) {
                        Collections.sort(timeMetric.getValue());
//                        Double max = Collections.max(timeMetric.getValue());
//                        for (int i = 0; i < timeMetric.getValue().size(); i++)
//                            timeMetric.getValue().set(i, Math.round( (timeMetric.getValue().get(i)/max * 100)*1000)/1000.0);
                        System.out.println(timeMetric.getKey() + " : ");
                        int ind50 = (int)(timeMetric.getValue().size()*0.5);
                        int ind75 = (int)(timeMetric.getValue().size()*0.75);
                        int ind90 = (int)(timeMetric.getValue().size()*0.9);
                        int ind99 = (int)(timeMetric.getValue().size()*0.99);
                        System.out.println("50% - " + timeMetric.getValue().get(ind50));
                        System.out.println("75% - " + timeMetric.getValue().get(ind75));
                        System.out.println("90% - " + timeMetric.getValue().get(ind90));
                        System.out.println("99% - " + timeMetric.getValue().get(ind99));
                       // System.out.println(timeMetric.getValue());

                    }
            }
        });

        // start the worker node to listen for assigned tasks from the master node
        worker.startThread();
    }

}
