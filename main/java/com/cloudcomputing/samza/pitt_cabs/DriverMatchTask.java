package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.Math.*;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import kafka.utils.Json;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

  /* Define per task state here. (kv stores etc) */

  // driverInformation provides all the information for the driver
  private KeyValueStore<String, Map<String, Map<String, Object>>> driverInformation;
  // contains number of available drivers in the past requests for each block

  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
         //Initialize stuff (maybe the kv stores?)
        driverInformation = (KeyValueStore<String, Map<String, Map<String, Object>>>) context.getStore("driver-loc");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    // The main part of your code. Remember that all the messages for a particular partition
    // come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
    // at one task only, thereby enabling you to do stateful stream processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // processing the stream with drive location
            processDriver_Location((Map<String, Object>) envelope.getMessage(),collector);
        
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // proecess the stream with event information
            processEvents((Map<String, Object>) envelope.getMessage(), collector);
        
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
  }

  // Process driver information
  private void processDriver_Location(Map<String, Object> message, MessageCollector collector) {
        // Retrieve information from message in JSON format
        String blockId_str = String.valueOf((Integer) message.get("blockId")); 
        String driverId_str = String.valueOf((Integer) message.get("driverId"));
        Integer latitude = (Integer) message.get("latitude");
        Integer longitude = (Integer) message.get("longitude");

        // Drivers in a block
        Map<String, Map<String, Object>> drivers;
        
        try{
             drivers = (Map<String, Map<String, Object>>) driverInformation.get(blockId_str);
        } catch (NullPointerException e) {
            drivers = null;
        }

        // If there's no driver list for this block
        if (drivers != null) {
        // If there is a list of driver for this block already, update the location
            if (drivers.containsKey(driverId_str)) {
                Map<String, Object> temp = drivers.get(driverId_str);
                temp.put("latitude", latitude);
                temp.put("longitude", longitude);
               
            } 
        }
    }

// Process all event types
private void processEvents(Map<String, Object> message, MessageCollector collector) {
        // Get general information
        String blockId_str = String.valueOf((Integer) message.get("blockId")); 
        String type = (String) message.get("type");
        Integer latitude = (Integer) message.get("latitude");
        Integer longitude = (Integer) message.get("longitude");


        if (type.equals("LEAVING_BLOCK")) {
            
            String driverId_str = String.valueOf((Integer) message.get("driverId"));
            String status = (String) message.get("status");
            
            // Remove the driver from the driver list for this block
            if (status != null) {
                Map<String, Map<String, Object>> drivers = (Map<String, Map<String, Object>>) driverInformation.get(blockId_str);
                if (drivers != null) {
                  drivers.remove(driverId_str);
                  driverInformation.put(blockId_str, drivers);
                }
            } else {
                throw new IllegalStateException("Unexpected status with: " + type);
            }

        } else if (type.equals("ENTERING_BLOCK")) {

            String driverId_str = String.valueOf((Integer) message.get("driverId"));
            String status = (String) message.get("status");

            // when the driver is available, put the driver back to the driver list for a block
            if (status.equals("AVAILABLE")) {

                Map<String, Map<String, Object>> drivers;
                try{
                     drivers = (Map<String, Map<String, Object>>) driverInformation.get(blockId_str);
                } catch (NullPointerException e) {
                    drivers = null;
                }

                // If there's no driver list for this block
                if (drivers == null) {
                    drivers = new HashMap<String, Map<String, Object>>();
                    drivers.put(driverId_str, message);
                    driverInformation.put(blockId_str, drivers);
                } else {
                // If there is a list of driver for this block already
                    if (!drivers.containsKey(driverId_str)) {
                        drivers.put(driverId_str, message);
                        driverInformation.put(blockId_str, drivers);
                    } 
                }

            } else if (status.equals("UNAVAILABLE")) {
                // Do nothing
            } else {
                throw new IllegalStateException("Unexpected status: " + type);
            }
        } else if (type.equals("RIDE_REQUEST")) {

            Integer clientId = (Integer) message.get("clientId");
            String gender_preference = (String) message.get("gender_preference");

            String seletedDriverId_str = null;
            int driverLatitude = 0;
            int driverLongitude = 0;

            double max_score = 0;
            double currentDistance = 0;
            boolean first = true;

            double distance_score = 0;
            double rating_score = 0;
            double salary_score = 0;
            double match_score = 0;
            double gender_score = 0;
            double MAX_DIST = Math.sqrt(500 * 500 * 2);

            // Assume each request will have at least oen driver, so no error handling here
            Map<String, Map<String, Object>> drivers = (Map<String, Map<String, Object>>) driverInformation.get(blockId_str);
            
            // Get driver message properties
            Map<String, Object> driverProperties;
            
            
            // Go through each available driver and find the driver with least distance
            for (String each : drivers.keySet()) {
                try{
                  driverProperties =  drivers.get(each);
                } catch (NullPointerException e) {
                  continue;
                }

                Double driverRating = (Double) driverProperties.get("rating");
                String driverGender = (String) driverProperties.get("gender");
                Integer driverSalary = (Integer) driverProperties.get("salary");

                driverLatitude = (Integer) driverProperties.get("latitude");
                driverLongitude = (Integer) driverProperties.get("longitude");

                // Calculate distance
                double distanceLatitude = Math.pow((driverLatitude - latitude), 2);
                double distanceLongtitude = Math.pow((driverLongitude - longitude), 2);
                currentDistance = Math.sqrt(distanceLatitude + distanceLongtitude);

                distance_score = 1 - (currentDistance) / MAX_DIST;
                rating_score = driverRating / 5.0;
                salary_score = 1 - driverSalary / 100.0;
                
                if (gender_preference.equals("N") || gender_preference.equals(driverGender)) {
                  gender_score = 1;
                } 

                match_score =  distance_score * 0.4 + gender_score * 0.2 + rating_score * 0.2 + salary_score * 0.2;

                if (first) {
                    max_score = match_score;
                    seletedDriverId_str = each;
                    first = false;
                } else if (max_score < match_score) {
                    max_score = match_score;
                    seletedDriverId_str = each;
                }
                
            }

            if(seletedDriverId_str != null) {
                // Once the driver is selected, remove it from the driver list
                drivers.remove(seletedDriverId_str);
                driverInformation.put(blockId_str, drivers);
               
                Map<String, Object> outputMap = new HashMap<String, Object>();
                Integer selectedDriverId = Integer.parseInt(seletedDriverId_str);

                //Output the pair
                outputMap.put("clientId", clientId);
                outputMap.put("driverId", selectedDriverId);
                collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, outputMap));
            }
        
        } else if (type.equals("RIDE_COMPLETE")) {

            String driverId_str = String.valueOf((Integer) message.get("driverId"));
            
            Map<String, Map<String, Object>> drivers;
            try{
                 drivers = (Map<String, Map<String, Object>>) driverInformation.get(blockId_str);
            } catch (NullPointerException e) {
                drivers = null;
            }

            // If there's no driver list for this block
            if (drivers == null) {
                drivers = new HashMap<String, Map<String, Object>>();
                drivers.put(driverId_str, message);
                driverInformation.put(blockId_str, drivers);      
            } else {
            // If there is a list of driver for this block already
                if (!drivers.containsKey(driverId_str)) {
                    drivers.put(driverId_str, message);
                    driverInformation.put(blockId_str, drivers);
                } 
            }
            
        } else {
            throw new IllegalStateException("Unexpected type : " + type);
        }
    }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    //this function is called at regular intervals, not required for this project
  }
}


