/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class DBWrapper extends DB
{
  private DB _db;
  private Measurements _measurements;

  private boolean reportLatencyForEachError = false;
  private HashSet<String> latencyTrackedErrors = new HashSet<String>();

  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY =
      "reportlatencyforeacherror";
  private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT =
      "false";

  private static final String LATENCY_TRACKED_ERRORS_PROPERTY =
      "latencytrackederrors";

  // To prevent exiting before all of the requests have actually executed,
  // queue up the requests and wait for them in responseThread. We signal
  // that work is done by sending null into workq in cleanup().
  private final BlockingQueue<Optional<ListenableFuture<Status>>> workq = Queues.newLinkedBlockingQueue();
  private Thread responseThread = null;

  public DBWrapper(DB db)
  {
    _db=db;
    _measurements=Measurements.getMeasurements();
  }

  /**
   * Set the properties for this DB.
   */
  public void setProperties(Properties p)
  {
    _db.setProperties(p);
  }

  /**
   * Get the set of properties for this DB.
   */
  public Properties getProperties()
  {
    return _db.getProperties();
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException
  {
    responseThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          Optional<ListenableFuture<Status>> s = null;
          while (true) {
            try {
              s = workq.take();
              break;
            } catch (InterruptedException e) {
            }
          }
          if (!s.isPresent()) {
            // no more work
            return;
          }
          Futures.getUnchecked(s.get());
        }
      }
    });
    responseThread.start();

    _db.init();

    this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
        getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
            REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

    if (!reportLatencyForEachError) {
      String latencyTrackedErrors = getProperties().getProperty(
          LATENCY_TRACKED_ERRORS_PROPERTY, null);
      if (latencyTrackedErrors != null) {
        this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
            latencyTrackedErrors.split(",")));
      }
    }

    System.err.println("DBWrapper: report latency for each error is " +
        this.reportLatencyForEachError + " and specific error codes to track" +
        " for latency are: " + this.latencyTrackedErrors.toString());
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException
  {
    while (true) {
      try {
        workq.put(Optional.<ListenableFuture<Status>>absent());
        responseThread.join();
        break;
      } catch (InterruptedException e) {}
    }
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    _db.cleanup();
    long en=System.nanoTime();
    measure("CLEANUP", Status.OK, ist, st, en);
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  public ListenableFuture<Status> read(String table, String key, Set<String> fields,
                                      HashMap<String,ByteIterator> result)
  {
    final long ist=_measurements.getIntendedtartTimeNs();
    final long st = System.nanoTime();
    ListenableFuture<Status> res=_db.read(table,key,fields,result);
    addMeasureCallbackAndQueue(res, "READ", ist, st);
    return res;
  }

  private final void addMeasureCallbackAndQueue(ListenableFuture<Status> res, final String op, final long ist, final long st) {
    Futures.addCallback(res, new FutureCallback<Status>() {
      @Override
      public void onSuccess(Status status) {
        long en = System.nanoTime();
        measure(op, status, ist, st, en);
        _measurements.reportStatus(op, status);
      }

      @Override
      public void onFailure(Throwable throwable) {
        // shouldn't end up here
      }
    });

    while (true) {
      try {
        workq.put(Optional.of(res));
        break;
      } catch (InterruptedException e) {}
    }
  }

  /**
   * Perform a range scan for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  public ListenableFuture<Status> scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    ListenableFuture<Status> res=_db.scan(table,startkey,recordcount,fields,result);
    addMeasureCallbackAndQueue(res, "SCAN", ist, st);
    return res;
  }

  private synchronized void measure(String op, Status result, long intendedStartTimeNanos,
      long startTimeNanos, long endTimeNanos) {
    String measurementName = op;
    if (!result.equals(Status.OK)) {
      if (this.reportLatencyForEachError ||
          this.latencyTrackedErrors.contains(result.getName())) {
        measurementName = op + "-" + result.getName();
      } else {
        measurementName = op + "-FAILED";
      }
    }
    _measurements.measure(measurementName,
        (int)((endTimeNanos-startTimeNanos)/1000),
        result.wasTraced());
    _measurements.measureIntended(measurementName,
        (int)((endTimeNanos-intendedStartTimeNanos)/1000),
        result.wasTraced());
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  public ListenableFuture<Status> update(String table, String key,
      HashMap<String,ByteIterator> values)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    ListenableFuture<Status> res=_db.update(table,key,values);
    addMeasureCallbackAndQueue(res, "UPDATE", ist, st);
    return res;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  public ListenableFuture<Status> insert(String table, String key,
      HashMap<String,ByteIterator> values)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    ListenableFuture<Status> res=_db.insert(table,key,values);
    addMeasureCallbackAndQueue(res, "INSERT", ist, st);
    return res;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public ListenableFuture<Status> delete(String table, String key)
  {
    long ist=_measurements.getIntendedtartTimeNs();
    long st = System.nanoTime();
    ListenableFuture<Status> res=_db.delete(table,key);
    addMeasureCallbackAndQueue(res, "DELETE", ist, st);
    return res;
  }
}
