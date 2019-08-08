/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package scc.processors.demo;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.OutputStreamCallback;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Configuration;
import com.google.common.io.ByteSource;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.Charsets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.OutputStream;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConvertJsonToHQL extends AbstractProcessor {

    public static final PropertyDescriptor Validator = new PropertyDescriptor
            .Builder().name("Validator")
            .displayName("Validator")
            .description("Validation for schema")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("Success relation")
            .description("Pass on success")
            .build();
    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("Fail relation")
            .description("Fail to pass file")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(Validator);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAIL_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        /* Getting flow file from the session */
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        /* Convert flow file to json object */
        try{

            InputStream in = session.read(flowFile);

            ByteSource byteSource = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException {
                    return in;
                }
            };
            String json = byteSource.asCharSource(Charsets.UTF_8).read();

            Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);

            /* Extracting query information from json object */
            String queryType = JsonPath.read(document, "$.type");
            String dbName = JsonPath.read(document, "$.database");
            String tableName = JsonPath.read(document, "$.table_name");
            List<Object> columns = JsonPath.read(document, "$.columns[*].value");
            List<Object> oldColumns = JsonPath.read(document, "$.columns[*].last_value");
            List<String> columnNames = JsonPath.read(document, "$.columns[*].name");

            StringBuilder query = new StringBuilder();

            /* Building insert query */
            if(queryType.toLowerCase().equals("insert")){
                queryType += " into";
                query.append(queryType).append(" ").append(dbName).append(".").append(tableName);

                StringBuilder values = new StringBuilder();
                for(Object column : columns)
                {
                    if(isNumeric(column.toString())){
                        values = values.length() > 0 ? values.append(",").append(column.toString()) : values.append(column.toString());
                    } else{
                        values = values.length() > 0 ? values.append(",'").append(column.toString()).append("'") : values.append("'").append(column.toString()).append("'");
                    }
                }
                query.append(" values(" + values + ")");

            }
            /* Building delete query */
            else if(queryType.toLowerCase().equals("delete")){
                queryType += " from";
                query.append(queryType).append(" ").append(dbName).append(".").append(tableName).append(" where ");

                int i = 0;
                for(Object val: columns)
                {
                    if(isNumeric(val.toString())){
                        query.append(columnNames.get(i)).append(" = ").append(val.toString());
                    } else {
                        query.append(columnNames.get(i)).append(" = ").append("'").append(val.toString()).append("'");
                    }

                    if(i < (columns.size() - 1)){
                        query.append(" and ");
                    }
                    i ++;
                }

            }
            /* Building delete query */
            else if(queryType.toLowerCase().equals("update")){
                query.append(queryType).append(" ").append(dbName).append(".").append(tableName).append(" set ");

                int i = 0;
                for(Object val: columns)
                {
                    if(columnNames.get(i).toLowerCase().equals("id")){
                        i++;
                        continue;
                    }
                    if(isNumeric(val.toString())){
                        query.append(columnNames.get(i)).append(" = ").append(val.toString());
                    } else {
                        query.append(columnNames.get(i)).append(" = ").append("'").append(val.toString()).append("'");
                    }

                    if(i < (columns.size() - 1)){
                        query.append(" , ");
                    }
                    i++;
                }
                i = 0;
                query.append(" where ");
                for(Object val: oldColumns)
                {
                    if(isNumeric(val.toString())){
                        query.append(columnNames.get(i)).append(" = ").append(val.toString());
                    } else {
                        query.append(columnNames.get(i)).append(" = ").append("'").append(val.toString()).append("'");
                    }

                    if(i < (columns.size() - 1)){
                        query.append(" and ");
                    }
                    i ++;
                }

            } else{
                /* Failing case */
                /*Not handling DML commands left to be done later*/
                session.transfer(flowFile, FAIL_RELATIONSHIP);
                return;
            }

            /* Sucesses relationship */
            query.append(";");
            final String queryVal = query.toString();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(queryVal.getBytes(Charsets.UTF_8));
                    out.flush();
                }
            });
            session.transfer(flowFile, SUCCESS_RELATIONSHIP);

        } catch (IOException ioe){
            //IOEXception
        }

    }

    public static boolean isNumeric(String strNum) {
        return strNum.matches("-?\\d+(\\.\\d+)?");
    }
}