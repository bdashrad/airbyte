/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.base.output;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link OutputRecordConsumer} interface that uses a {@link PrintWriter} to
 * publish serialized {@link AirbyteMessage} objects. This implementation is not thread safe and it
 * is recommended to create a consumer per thread if used in a multithreaded way.
 */
@NotThreadSafe
public class PrintWriterOutputRecordConsumer implements OutputRecordConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrintWriterOutputRecordConsumer.class);

  private final PrintWriter writer;

  public PrintWriterOutputRecordConsumer() {
    this(new FileOutputStream(FileDescriptor.out));
  }

  @VisibleForTesting
  public PrintWriterOutputRecordConsumer(final OutputStream outputStream) {
    LOGGER.info("Using PrintWriter for output record collection.");
    writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)), true);
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("Closing PrintWriter...");
    writer.close();
    LOGGER.info("PrintWriter closed.");
  }

  @Override
  public void accept(final AirbyteMessage airbyteMessage) {
    final String json = Jsons.serialize(airbyteMessage);
    LOGGER.debug("Accepted message '{}' for output.", json);
    writer.println(json);
  }

}
