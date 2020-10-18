/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.Option;
import org.apache.avro.generic.GenericRecord;

public class DebeziumAvroPayload extends OverwriteWithLatestAvroPayload {

  // Field is prefixed with a underscore by transformer to indicate metadata field
  public static final String OP_FIELD = "_op";
  public static final String DELETE_OP = "d";

  public DebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public DebeziumAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    return (genericRecord.get(OP_FIELD) != null && genericRecord.get(OP_FIELD).toString().equalsIgnoreCase(
        DELETE_OP));
  }
}
