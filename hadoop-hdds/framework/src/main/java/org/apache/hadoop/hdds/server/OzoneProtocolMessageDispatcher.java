/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.server;

import org.apache.hadoop.hdds.function.FunctionWithServiceException;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.protocolPB.ProtocolMessageMetrics;

import com.google.protobuf.ServiceException;
import io.opentracing.Scope;
import org.slf4j.Logger;

import java.util.function.UnaryOperator;

/**
 * Dispatch message after tracing and message logging for insight.
 * <p>
 * This is a generic utility to dispatch message in ServerSide translators.
 * <p>
 * It logs the message type/content on DEBUG/TRACING log for insight and create
 * a new span based on the tracing information.
 */
@SuppressWarnings("squid:S00119")
public class OzoneProtocolMessageDispatcher<REQUEST, RESPONSE> {

  private static final String PREFIX = "[service={}] [type={}] ";
  private static final String JSON_TAG = "<json>{}</json>";
  private static final String REQUEST_DEBUG_FORMAT = PREFIX +
      "request is received";
  private static final String REQUEST_TRACE_FORMAT = PREFIX +
      "request is received: " + JSON_TAG;
  private static final String RESPONSE_TRACE_FORMAT = PREFIX +
      "request is processed. Response: " + JSON_TAG;

  private String serviceName;

  private final ProtocolMessageMetrics protocolMessageMetrics;

  private Logger logger;
  private final UnaryOperator<REQUEST> requestPreprocessor;
  private final UnaryOperator<RESPONSE> responsePreprocessor;

  public OzoneProtocolMessageDispatcher(String serviceName,
      ProtocolMessageMetrics protocolMessageMetrics, Logger logger) {
    this(serviceName, protocolMessageMetrics, logger, req -> req, resp -> resp);
  }

  public OzoneProtocolMessageDispatcher(String serviceName,
      ProtocolMessageMetrics protocolMessageMetrics, Logger logger,
      UnaryOperator<REQUEST> requestPreprocessor,
      UnaryOperator<RESPONSE> responsePreprocessor) {
    this.serviceName = serviceName;
    this.protocolMessageMetrics = protocolMessageMetrics;
    this.logger = logger;
    this.requestPreprocessor = requestPreprocessor;
    this.responsePreprocessor = responsePreprocessor;
  }

  public RESPONSE processRequest(
      REQUEST request,
      FunctionWithServiceException<REQUEST, RESPONSE> methodCall,
      Object type,
      String traceId) throws ServiceException {
    String msgType = type.toString();
    try (Scope ignored = TracingUtil.importAndCreateScope(msgType, traceId)) {
      if (logger.isTraceEnabled()) {
        logger.trace(REQUEST_TRACE_FORMAT, serviceName, msgType,
            escapeNewLines(requestPreprocessor.apply(request).toString()));
      } else if (logger.isDebugEnabled()) {
        logger.debug(REQUEST_DEBUG_FORMAT, serviceName, msgType);
      }
      protocolMessageMetrics.increment(type);

      RESPONSE response = methodCall.apply(request);

      if (logger.isTraceEnabled()) {
        logger.trace(RESPONSE_TRACE_FORMAT, serviceName, msgType,
            escapeNewLines(responsePreprocessor.apply(response).toString()));
      }

      return response;
    }
  }

  private static String escapeNewLines(String input) {
    return input.replaceAll("\n", "\\\\n");
  }
}
