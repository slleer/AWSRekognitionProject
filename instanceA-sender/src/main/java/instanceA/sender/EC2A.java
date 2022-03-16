package instanceA.sender;
/*
 *  Author: Stephen Leer
 *  Date: 10/22/21
 *  Version: 1.3
 */
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.List;

public class EC2A {

   public static void main(String[] args) throws Exception {

      String photo = "1.jpeg";
      String bucket = "unr-cs442";
      String QUEUE_NAME = "queue-CS442.fifo"; 


      AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
              .withRegion("us-west-2")
              .build();
      
      AmazonS3 s3 = AmazonS3ClientBuilder.standard()
              .withRegion("us-west-2")
              .build();
      
      AmazonSQS sqs = AmazonSQSClientBuilder.standard()
              .withRegion(Regions.US_WEST_2)
              .build();

      try {

    	  String queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

          System.out.println("Listing objects");
          ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                  .withBucketName(bucket));
          for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
              System.out.println(" - " + objectSummary.getKey() + "  " +
                                 "(size = " + objectSummary.getSize() + ")");
              photo = objectSummary.getKey();
              DetectLabelsRequest request = new DetectLabelsRequest()
                      .withImage(new Image()
                      .withS3Object(new S3Object()
                      .withName(photo).withBucket(bucket)))
                      .withMaxLabels(10)
                      .withMinConfidence(75F);
              DetectLabelsResult result = rekognitionClient.detectLabels(request);
              List <Label> labels = result.getLabels();
              
              System.out.println("Detected labels for " + photo);
              for (Label label: labels) {
            	  if (label.getName().equals("Car") && label.getConfidence() > 90.0) {
            		  System.out.println(label.getName() + ": " + label.getConfidence().toString());
            	        SendMessageRequest send_msg_request = new SendMessageRequest()
            	                .withQueueUrl(queueUrl)
            	                .withMessageBody(photo)
            	                .withMessageGroupId(queueUrl);
            	        sqs.sendMessage(send_msg_request);
            	  }
              }
          }
          SendMessageRequest send_msg_request = new SendMessageRequest()
        		  .withQueueUrl(queueUrl)
        		  .withMessageBody("-1")
        		  .withMessageGroupId(queueUrl);
          sqs.sendMessage(send_msg_request);
          System.out.println();
      } catch(AmazonRekognitionException e) {
         e.printStackTrace();
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