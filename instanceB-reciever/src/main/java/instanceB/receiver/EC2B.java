package instanceB.receiver;
/*
 *  Author: Stephen Leer
 *  Date: 10/22/21
 *  Version: 1.3
 */
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.TextDetection;

public class EC2B {

    public static void main(String[] args) throws IOException {



        
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();
        
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                .withRegion("us-west-2")
                .build();        

        String bucketName = "unr-cs442";
        String key = "";
        String QUEUE_NAME = "queue-CS442.fifo";
        

        try {
        	
        	String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
        	
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            if(messages.size() <= 0) {
            	System.out.println("NO MESSAGES TO RECIEVE!");
            	System.exit(0);
            }
            key = messages.get(0).getBody();
            FileWriter out = new FileWriter("output.txt", false);
            if("-1".equals(key)) {
                String messageReceiptHandle = messages.get(0).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
            }
        	while(!"-1".equals(key)) {
        		for (Message message : messages) {
        			
        		    DetectTextRequest request = new DetectTextRequest()
        		    		.withImage(new Image()
        		            .withS3Object(new S3Object()
        		            .withName(key)
        		            .withBucket(bucketName)));
        			System.out.println("    Body:          " + message.getBody());
        			DetectTextResult result = rekognitionClient.detectText(request);
        	        List<TextDetection> textDetections = result.getTextDetections();

        	        if (textDetections.size() > 0) {
        	        	out.write(key + " has Car(s) and Text:\n");
        	        	System.out.println(key + " has Cars and text:");
        	        }
        	        for (TextDetection text: textDetections) {
        	      
        	        	out.write(" " + text.getDetectedText());
        	        	System.out.print(" " + text.getDetectedText());

        	        }
        	        out.write("\n");

        		}
        		System.out.println("\nDeleting the message: " + key);
                String messageReceiptHandle = messages.get(0).getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
                receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
                messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                key = messages.get(0).getBody();
        	}
        	out.close();
            String messageReceiptHandle = messages.get(0).getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));

            System.out.println();



        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
