package com.amazonaws.samples;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;

public class LocalAwsApp {
	private static final String BUCKET_NAME = "ass1filebucket";
	private static final String MANAGER_QUEUE = "https://sqs.us-west-2.amazonaws.com/014344929412/MANAGER_QUEUE";
	private static final String MANAGER_FINISH_QUEUE = "https://sqs.us-west-2.amazonaws.com/014344929412/MANAGER_FINISH_QUEUE";

	public static void main(String[] args) {
		String inputFileName = "sampleInput.txt";
		//String outputFileName = args[1];
		//int n = Integer.parseInt(args[2]);

        System.out.println("===========================================");
        System.out.println("Getting Started with Local AWS App");
        System.out.println("===========================================\n");

		//get AWS credentials
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_1)
                .build();
        
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_1)
                .build();

        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        
		try {
        CreateManagerNodeIfNeeded(ec2); 
        CreateS3BucketIfNeeded(s3);
        String key = UploadFileToS3IfNeeded(s3, inputFileName);
        CreateSQSqueueIfNeeded(sqs);
        ClearAll(s3, key, sqs);
        
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
    	} catch (AmazonClientException ace) {
        System.out.println("Caught an AmazonClientException, which means the client encountered " +
                "a serious internal problem while trying to communicate with SQS, such as not " +
                "being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    }
    }
		
	//-------------------------------------------------------------
	//------------------------EC2 Functions------------------------
	//-------------------------------------------------------------
	
	private static void CreateManagerNodeIfNeeded(AmazonEC2 ec2) {
    	List<Reservation> reservationList = ec2.describeInstances().getReservations(); 
    	for (Reservation reservation : reservationList) {
    		List<Instance> instancesList =  reservation.getInstances();
    		for (Instance instance : instancesList) {
    			for (Tag tag : instance.getTags()) {
					if(tag.getKey().equals("Type") && tag.getValue().equals("Manager")) {
						// found manager instance
						int instanceCode = instance.getState().getCode();
						if(instanceCode == 16 || instanceCode == 0) {
							// Code 16 = running, 
							// Code 0 = pending
							System.out.println("Manager is running, state code: " + instanceCode);
							return;
						}
						if(instanceCode == 64 || instanceCode == 80) {
							// Code 64 = stopping,
							// Code 80 = stopped
							// should terminate Instance
							System.out.println("terminate manager instance, state code is :" + instance.getState().getCode());
							TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest()
									.withInstanceIds(instance.getInstanceId());
						    ec2.terminateInstances(terminateRequest);
						}
					}
				}
			}
		}
    	RunNewManagerInstance(ec2);
	}
	
	private static void RunNewManagerInstance(AmazonEC2 ec2) {
		System.out.println("Run new manager instance/n");
		
		// make manager instance
        RunInstancesRequest request = new RunInstancesRequest("ami-0080e4c5bc078760e", 1, 1);
        request.setInstanceType(InstanceType.T2Micro.toString());

	    // set tags
	    List<Tag> tags = new ArrayList<Tag>();
	    tags.add(new Tag().withKey("Type").withValue("Manager"));
	    tags.add(new Tag().withKey("Name").withValue("Manager"));
	    TagSpecification tagSpec = new TagSpecification().withTags(tags).withResourceType("instance");
	    request.setTagSpecifications(Arrays.asList(tagSpec));
	    
	    
	    //Launch instance of Manager
        List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
        System.out.println("Launch instance of manager: " + instances);
	}
	
	//-------------------------------------------------------------
	//------------------------S3 Functions-------------------------
	//-------------------------------------------------------------
	
	private static void CreateS3BucketIfNeeded(AmazonS3 s3) {
		System.out.println("Listing buckets on S3");
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(" -> " + bucket.getName());
            if(bucket.getName().equals(BUCKET_NAME)) {
            	System.out.println("bucket found, no need to create new bucket: " + BUCKET_NAME);
            	return;
            }
        }//bucket not found, create a new one 
        System.out.println("create new bucket " + BUCKET_NAME);
        s3.createBucket(BUCKET_NAME);
	}
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
 
            System.out.println("    " + line);
        }
        System.out.println();
    }
    
	private static String UploadFileToS3IfNeeded(AmazonS3 s3, String inputFileName) {
        File inputFile = new File(inputFileName);
        String key = inputFile.getName().replace('\\', '_').replace('/','_').replace(':', '_');
        
        //delete object if already exist
        System.out.println("Listing objects and delete if existing object if needed");
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(BUCKET_NAME));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() + ")");
            if(objectSummary.getKey().equals(inputFileName)) {
                System.out.println("Deleting an object\n" + inputFileName);
                s3.deleteObject(BUCKET_NAME, key);
            }
        }

		//Upload the new file to S3
		System.out.println("Uploading a new file to S3");
        PutObjectRequest req = new PutObjectRequest(BUCKET_NAME, key, inputFile);
        s3.putObject(req);
        
        //download the object and stream content
//        System.out.println("Downloading an object");
//        S3Object object = s3.getObject(new GetObjectRequest(BUCKET_NAME, key));
//        System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
//        try {
//			displayTextInputStream(object.getObjectContent());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        return key;
	}
	
	//-------------------------------------------------------------
	//------------------------EC2 Functions------------------------
	//-------------------------------------------------------------
	
	private static void CreateSQSqueueIfNeeded(AmazonSQS sqs) {
		boolean hasManagerQueue = false;
		boolean hasManagerFinishQueue = false;
	    // check if queues aleady exists
	    for (String queueUrl : sqs.listQueues().getQueueUrls()) {
	    	if(new String(queueUrl).equals(MANAGER_QUEUE)) {
	    		hasManagerQueue = true;
	    	}
	        if(new String(queueUrl).equals(MANAGER_FINISH_QUEUE)) {
	            hasManagerFinishQueue = true;
	        }
	        }
	        if(!hasManagerQueue) {
	            System.out.println("Creating a new SQS MANAGER_QUEUE.\n");
	            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MANAGER_QUEUE");
	            sqs.createQueue(createQueueRequest);
	        } else {
	        	System.out.println("MANAGER_QUEUE already exist.\n");
	        }
	        if(!hasManagerFinishQueue) {
	            System.out.println("Creating a new SQS MANAGER_FINISH_QUEUE.\n");
	            CreateQueueRequest createQueueRequest = new CreateQueueRequest("MANAGER_FINISH_QUEUE");
	            sqs.createQueue(createQueueRequest);
	        } else {
	        	System.out.println("MANAGER_FINISH_QUEUE already exist.\n");
	        }
	}
	
	private static void ClearAll(AmazonS3 s3, String key, AmazonSQS sqs) {
        /*
         * Delete an object - Unless versioning has been turned on for your bucket,
         * there is no way to undelete an object, so use caution when deleting objects.
         */
        System.out.println("Deleting an object\n");
        s3.deleteObject(BUCKET_NAME, key);

        /*
         * Delete a bucket - A bucket must be completely empty before it can be
         * deleted, so remember to delete any objects from your buckets before
         * you try to delete them.
         */
        System.out.println("Deleting bucket " + BUCKET_NAME + "\n");
        s3.deleteBucket(BUCKET_NAME);
        
        sqs.deleteQueue(new DeleteQueueRequest(MANAGER_QUEUE));
        sqs.deleteQueue(new DeleteQueueRequest(MANAGER_FINISH_QUEUE));
	}

}

