package examples.calc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import messaging.QueueCalc.CalcService;
import messaging.QueueCalc.MessageResult;
import messaging.QueueCalc.RequestMessage;
import taskqueue.Worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * WordCountWorker is a implementation of a worker node that receives Strings
 * and tally up the number of words contained and return back the count
 *
 * @author paulcao
 *
 */
public class CalcWorker {
    boolean flag = true;

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
    public CalcWorker(String host, int port,
                           String senderHost, int senderPort, int io_threads, int nodeId, Map<String, ArrayList<Double>> measureTimeMap){
        Worker worker = new Worker("127.0.0.1", 5555, "127.0.0.1", 5556, 1);
        worker.registerService(new CalcService(){
            @Override
            public void add(RpcController controller,
                            RequestMessage request, RpcCallback<MessageResult> done) {
                long receivedRequestTime = System.currentTimeMillis();
                int result = request.getNumberA() + request.getNumberB();
                long sentRequestTime = request.getStartTime();
                MessageResult resultMessage = MessageResult.newBuilder().setMessage(result).build();
                if(result %10 == 3){
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if(result==0 && flag){
                    showResults();
                }
                if (done != null) {
                    done.run(resultMessage);
                    long sentReplyTime = System.currentTimeMillis();
                    addMetrics(measureTimeMap,sentRequestTime,sentReplyTime,receivedRequestTime, "+");
                }
            }

            @Override
            public void subtract(RpcController controller, RequestMessage request, RpcCallback<MessageResult> done) {
                long receivedRequestTime = System.currentTimeMillis();
                int result = request.getNumberA() - request.getNumberB();
                long sentRequestTime = request.getStartTime();
                MessageResult resultMessage = MessageResult.newBuilder().setMessage(result).build();
                if(result==0 && flag) {
                    showResults();
                }
                if (done != null) {
                    done.run(resultMessage);
                    long sentReplyTime = System.currentTimeMillis();
                    addMetrics(measureTimeMap,sentRequestTime,sentReplyTime,receivedRequestTime, "-");
                }
            }

            private void addMetrics(Map<String, ArrayList<Double>> measureTimeMap, long sentRequestTime, long sentReplyTime, long receivedRequestTime, String func) {
                if(!measureTimeMap.containsKey("WorkerProcessingTime for " + func)){
                    ArrayList<Double> workerProcessing = new ArrayList<>();
                    workerProcessing.add((double)(sentReplyTime - receivedRequestTime));
                    measureTimeMap.put("WorkerProcessingTime for " + func ,workerProcessing);
                }else{
                    measureTimeMap.get("WorkerProcessingTime for " + func).add((double)(sentReplyTime - receivedRequestTime));
                }
                if(!measureTimeMap.containsKey("MessageProcessingTime for " + func)){
                    ArrayList<Double> messageProcessing = new ArrayList<>();
                    messageProcessing.add((double)sentReplyTime - sentRequestTime);
                    measureTimeMap.put("MessageProcessingTime for " + func,messageProcessing);
                }else{
                    measureTimeMap.get("MessageProcessingTime for " + func ).add((double)sentReplyTime-sentRequestTime);
                }

                if(!measureTimeMap.containsKey("QueueWaitTime for " + func)){
                    ArrayList<Double> timeDiff = new ArrayList<>();
                    timeDiff.add((double)receivedRequestTime-sentRequestTime);
                    measureTimeMap.put("QueueWaitTime for " + func,timeDiff);
                }else{
                    measureTimeMap.get("QueueWaitTime for " + func).add((double)receivedRequestTime-sentRequestTime);
                }
            }

            private void showResults() {
                flag=false;
                for (Map.Entry<String, ArrayList<Double>> timeMetric:
                        measureTimeMap.entrySet()) {
                    Collections.sort(timeMetric.getValue());
                    System.out.println(timeMetric.getKey() + " : ");
                    int ind50 = (int)(timeMetric.getValue().size()*0.5);
                    int ind75 = (int)(timeMetric.getValue().size()*0.75);
                    int ind90 = (int)(timeMetric.getValue().size()*0.9);
                    int ind99 = (int)(timeMetric.getValue().size()*0.99);
                    System.out.println("50% - " + timeMetric.getValue().get(ind50));
                    System.out.println("75% - " + timeMetric.getValue().get(ind75));
                    System.out.println("90% - " + timeMetric.getValue().get(ind90));
                    System.out.println("99% - " + timeMetric.getValue().get(ind99));
                }
            }
        });

        // start the worker node to listen for assigned tasks from the master node
        worker.startThread();
    }

}
