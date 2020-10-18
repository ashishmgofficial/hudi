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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.spark.api.java.JavaSparkContext;

public class DebeziumSchemaRegistryProvider extends SchemaRegistryProvider {

  public DebeziumSchemaRegistryProvider(TypedProperties props,
                                        JavaSparkContext jssc) {
    super(props, jssc);
  }

  /**
   * Debezium target schema is a nested structure with many metadata fields. This will
   * flatten the schema structure and only require necessary metadata information
   * @return
   */
  @Override
  public Schema getTargetSchema() {
    Schema registrySchema = super.getTargetSchema();

    Field dataField = registrySchema.getField("after");
    Field tsField = registrySchema.getField("ts_ms");
    Field opField = registrySchema.getField("op");

    // Initialize with metadata columns
    FieldAssembler<Schema> payloadFieldAssembler = SchemaBuilder.builder()
        .record("formatted_debezium_payload")
        .fields()
        .name("_" + tsField.name()).type(tsField.schema()).withDefault(null)
        .name("_" + opField.name()).type(opField.schema()).withDefault(null);

    // Add data columns to schema
    dataField.schema()
        .getTypes()
        // "after" field is a union with data schema and null schema, so we need to extract only the data schema portion
        .get(dataField.schema().getIndexNamed(registrySchema.getNamespace() + ".Value"))
        .getFields()
        .forEach(field -> {
          payloadFieldAssembler.name(field.name()).type(field.schema()).withDefault(null);
        });

    Schema schema = payloadFieldAssembler.endRecord();
    Schema newSchema = AvroConversionUtils.convertStructTypeToAvroSchema(
        AvroConversionUtils.convertAvroSchemaToStructType(schema), RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME,
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE);
    LOG.info("Transformed Avro Schema is :" + schema.toString(true));
    LOG.info("Transformed Avro Schema after converting :" + newSchema.toString(true));
    return newSchema;
  }
}