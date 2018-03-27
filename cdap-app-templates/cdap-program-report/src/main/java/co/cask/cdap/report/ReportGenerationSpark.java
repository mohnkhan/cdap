/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.report;

import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.service.AbstractSparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import co.cask.cdap.report.proto.FilterDeserializer;
import co.cask.cdap.report.proto.ReportContent;
import co.cask.cdap.report.proto.ReportGenerationInfo;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.proto.ReportList;
import co.cask.cdap.report.proto.ReportStatus;
import co.cask.cdap.report.proto.ReportStatusInfo;
import co.cask.cdap.report.util.ProgramRunMetaFileUtil;
import co.cask.cdap.report.util.ReportField;
import co.cask.cdap.report.util.ReportIds;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A Spark program for generating reports, querying for report statuses, and reading reports.
 */
public class ReportGenerationSpark extends AbstractExtendedSpark implements JavaSparkMain {
  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationSpark.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ReportGenerationRequest.Filter.class, new FilterDeserializer())
    .create();

  @Override
  protected void configure() {
    setMainClass(ReportGenerationSpark.class);
    addHandlers(new ReportSparkHandler());
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
  }

  /**
   * A {@link SparkHttpServiceHandler} generating reports, querying for report statuses, and reading reports.
   */
  public static final class ReportSparkHandler extends AbstractSparkHttpServiceHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReportSparkHandler.class);
    private static final Type REPORT_GENERATION_REQUEST_TYPE = new TypeToken<ReportGenerationRequest>() {
    }.getType();
    private static final int MAX_LIMIT = 10000;
    private static final String DEFAULT_LIMIT = "10000";
    private SparkSession sparkSession;

    public static final String START_FILE = "_START";
    public static final String REPORT_DIR = "reports";
    public static final String COUNT_FILE = "COUNT";
    public static final String SUCCESS_FILE = "_SUCCESS";
    public static final String FAILURE_FILE = "_FAILURE";

    @Override
    public void initialize(SparkHttpServiceContext context) throws Exception {
      super.initialize(context);
      try {
        // TODO: [CDAP-13216] temporarily create the run meta fileset and generate mock program run meta files here.
        // Will remove once the TMS subscriber writing to the run meta fileset is implemented.
        context.getAdmin().createDataset(ReportGenerationApp.RUN_META_FILESET, FileSet.class.getName(),
                                         FileSetProperties.builder().build());
        ProgramRunMetaFileUtil.populateMetaFiles(getDatasetBaseLocation(ReportGenerationApp.RUN_META_FILESET));
      } catch (InstanceConflictException e) {
        // It's ok if the dataset already exists
      }
      sparkSession = new SQLContext(getContext().getSparkContext()).sparkSession();
    }

    @GET
    @Path("/reports")
    public void getReports(HttpServiceRequest request, HttpServiceResponder responder,
                           @QueryParam("offset") @DefaultValue("0") int offset,
                           @QueryParam("limit")  @DefaultValue(DEFAULT_LIMIT) int limit)
      throws IOException {
      Location reportFilesetLocation = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET);
      List<ReportStatusInfo> reportStatuses = new ArrayList<>();
      // The index of the report directory to start reading from, initialized to the given offset
      int idx = offset;
      List<Location> reportBaseDirs = reportFilesetLocation.list();
      // Keep add report status information to the list until the index is no longer smaller than
      // the number of report directories or the list is reaching the given limit
      while (idx < reportBaseDirs.size() && reportStatuses.size() < limit) {
        Location reportBaseDir = reportBaseDirs.get(idx++);
        String reportId = reportBaseDir.getName();
        // Report ID is time based UUID. Get the creation time from the report ID.
        long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
        reportStatuses.add(new ReportStatusInfo(reportId, creationTime, getReportStatus(reportBaseDir)));
      }
      responder.sendJson(200, new ReportList(offset, limit, reportBaseDirs.size(), reportStatuses));
    }

    @GET
    @Path("/reports/{report-id}")
    public void getReportStatus(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("report-id") String reportId,
                                @QueryParam("share-id") String shareId)
      throws IOException {
      Location reportBaseDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportBaseDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      long creationTime = ReportIds.getTime(reportId, TimeUnit.SECONDS);
      // Read the report request from _START file, which was written at the beginning of report generation
      String reportRequest =
        new String(ByteStreams.toByteArray(reportBaseDir.append(START_FILE).getInputStream()), Charsets.UTF_8);
      responder.sendJson(new ReportGenerationInfo(creationTime, getReportStatus(reportBaseDir), reportRequest));
    }

    @GET
    @Path("reports/{report-id}/runs")
    public void getReportRuns(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("report-id") String reportId,
                              @QueryParam("offset") @DefaultValue("0") long offset,
                              @QueryParam("limit") @DefaultValue(DEFAULT_LIMIT) int limit,
                              @QueryParam("share-id") String shareId) throws IOException {
      if (offset < 0) {
        responder.sendError(400, "offset cannot be negative");
        return;
      }
      if (limit <= 0) {
        responder.sendError(400, "limit must be a positive integer");
        return;
      }
      if (limit > MAX_LIMIT) {
        responder.sendError(400, "limit must cannot be larger than " + MAX_LIMIT);
        return;
      }
      Location reportBaseDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      if (!reportBaseDir.exists()) {
        responder.sendError(404, String.format("Report with id %s does not exist.", reportId));
        return;
      }
      // Get the status of the report and only COMPLETED report can be read
      ReportStatus status = getReportStatus(reportBaseDir);
      if (!ReportStatus.COMPLETED.equals(status)) {
        responder.sendError(400, String.format("Report with id %s with status %s cannot be read.", reportId, status));
        return;
      }
      List<String> reportRecords = new ArrayList<>();
      long lineCount = 0;
      Location reportDir = reportBaseDir.append(REPORT_DIR);
      Location reportFile = null;
      // TODO: assume only one report file for now
      for (Location file : reportDir.list()) {
        if (file.getName().endsWith(".json")) {
          reportFile = file;
          break;
        }
      }
      if (reportFile == null) {
        responder.sendError(500, "No files found for report " + reportId);
        return;
      }
      // Read the report file and add lines starting from the position of offset to the result until the result reaches
      // the limit
      try (BufferedReader br = new BufferedReader(new InputStreamReader(reportFile.getInputStream()))) {
        String line;
        while ((line = br.readLine()) != null) {
          // skip lines before the offset
          if (lineCount++ < offset) {
            continue;
          }
          if (reportRecords.size() == limit) {
            break;
          }
          reportRecords.add(line);
        }
      }
      // Get the total number of records from the COUNT file
      String total =
        new String(ByteStreams.toByteArray(reportBaseDir.append(COUNT_FILE).getInputStream()), Charsets.UTF_8);
      responder.sendJson(200, new ReportContent(offset, limit, Integer.parseInt(total), reportRecords));
    }

    @POST
    @Path("/reports")
    public void executeReportGeneration(HttpServiceRequest request, HttpServiceResponder responder)
      throws IOException {
      String requestJson = Charsets.UTF_8.decode(request.getContent()).toString();
      LOG.debug("Received report generation request {}", requestJson);
      ReportGenerationRequest reportRequest;
      try {
        reportRequest = decodeRequestBody(requestJson, REPORT_GENERATION_REQUEST_TYPE);
        reportRequest.validate();
      } catch (IllegalArgumentException e) {
        responder.sendError(400, e.getMessage());
        return;
      }

      String reportId = ReportIds.generate().toString();
      Location reportBaseDir = getDatasetBaseLocation(ReportGenerationApp.REPORT_FILESET).append(reportId);
      reportBaseDir.mkdirs();
      LOG.debug("Created report base directory {} for report {}", reportBaseDir, reportId);
      // Create a _START file to indicate the start of report generation
      Location startFile = reportBaseDir.append(START_FILE);
      startFile.createNew();
      // Save the report generation request in the _START file
      try (PrintWriter writer = new PrintWriter(startFile.getOutputStream())) {
        writer.write(requestJson);
      }
      LOG.debug("Wrote to startFile {}", startFile.toURI());
      // Generate the report asynchronously in a new thread
      Executors.newSingleThreadExecutor().submit(() -> {
        try {
          generateReport(reportRequest, reportBaseDir);
        } catch (Throwable t) {
          LOG.error("Failed to generate report {}", reportId, t);
          try {
            // Write to the failure file in case of any exception occurs during report generation
            Location failureFile = reportBaseDir.append(FAILURE_FILE);
            failureFile.createNew();
            try (PrintWriter writer = new PrintWriter(failureFile.getOutputStream())) {
              writer.println(t.toString());
              t.printStackTrace(writer);
            }
          } catch (Throwable t2) {
            LOG.error("Failed to write cause of failure to file for report {}", reportId, t2);
            throw new RuntimeException("Failed to write cause of failure to file for report " + reportId, t2);
          }
        }
      });
      responder.sendJson(200, ImmutableMap.of("id", reportId));
    }

    /**
     * Generates report files according to the given request and write them to the given location.
     * Program run meta files are first filtered to exclude unnecessary files for report generation,
     * and send the paths of qualified run meta files to {@link ReportGenerationHelper#generateReport}
     * that actually launches a Spark job to generate reports.
     *
     * @param reportRequest request
     * @param reportBaseDir location of the directory where the report files directory, COUNT file,
     *                      and _SUCCESS file will be created.
     */
    private void generateReport(ReportGenerationRequest reportRequest, Location reportBaseDir) throws IOException {
      Location baseLocation = Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(ReportGenerationApp.RUN_META_FILESET).getBaseLocation();
      });
      // Get a list of directories of all namespaces under RunMetaFileset base location
      List<Location> nsLocations;
      nsLocations = baseLocation.list();
      // Get the namespace filter from the request if it exists
      ReportGenerationRequest.ValueFilter<String> namespaceFilter = null;
      if (reportRequest.getFilters() != null) {
        for (ReportGenerationRequest.Filter filter : reportRequest.getFilters()) {
          if (ReportField.NAMESPACE.getFieldName().equals(filter.getFieldName())) {
            // ReportGenerationRequest is validated to contain only one filter for namespace field
            namespaceFilter = (ReportGenerationRequest.ValueFilter<String>) filter;
            LOG.debug("Found namespace filter {}", namespaceFilter);
            break;
          }
        }
      }
      final ReportGenerationRequest.ValueFilter<String> nsFilter = namespaceFilter;
      Stream<Location> filteredNsLocations = nsLocations.stream();
      // If the namespace filter exists, apply the filter to get filtered namespace directories
      if (nsFilter != null) {
        filteredNsLocations = nsLocations.stream().filter(nsLocation -> nsFilter.apply(nsLocation.getName()));
      }
      // Iterate through all qualified namespaces directories to get program run meta files
      Stream<Location> metaFiles = filteredNsLocations.flatMap(nsLocation -> {
        try {
          List<Location> metaFileLocations = nsLocation.list();
          LOG.debug("Files under namespace {}: {}", nsLocation.getName(), metaFileLocations);
          return metaFileLocations.stream();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      // Program run meta files are in avro format. Each file is named by the earliest program run meta record
      // in the file, so exclude the files with no record earlier than the end of query time range.
      List<String> metaFilePaths = metaFiles.filter(metaFile -> {
        String fileName = metaFile.getName();
        return fileName.endsWith(".avro")
          && Long.parseLong(fileName.substring(0, fileName.indexOf(".avro"))) < reportRequest.getEnd();
      }).map(location -> location.toURI().toString()).collect(Collectors.toList());
      LOG.debug("Filtered meta files {}", metaFiles);
      // Generate the report with the request and program run meta files
      ReportGenerationHelper.generateReport(sparkSession, reportRequest, metaFilePaths, reportBaseDir);
    }

    /**
     * Returns the status of the report generation by checking the presence of the success file or the failure file.
     * If neither of these files exists, the report generation is still running.
     *
     * TODO: [CDAP-13215] failure file may not be written if the Spark program is killed. Status of killed
     * report generation job might be returned as RUNNING
     *
     * @param reportBaseDir the base directory with report ID as directory name
     * @return status of the report generation
     */
    private ReportStatus getReportStatus(Location reportBaseDir) throws IOException {
      if (reportBaseDir.append(SUCCESS_FILE).exists()) {
        return ReportStatus.COMPLETED;
      }
      if (reportBaseDir.append(FAILURE_FILE).exists()) {
        return ReportStatus.FAILED;
      }
      return ReportStatus.RUNNING;
    }

    private Location getDatasetBaseLocation(String datasetName) {
      return Transactionals.execute(getContext(), context -> {
        return context.<FileSet>getDataset(datasetName).getBaseLocation();
      });
    }

    private  <T> T decodeRequestBody(String request, Type type) {
      T decodedRequestBody;
      try {
        decodedRequestBody = GSON.fromJson(request, type);
        if (decodedRequestBody == null) {
          throw new IllegalArgumentException("Request body cannot be empty.");
        }
      } catch (JsonSyntaxException e) {
        throw new IllegalArgumentException("Request body is invalid json: " + e.getMessage());
      }
      return decodedRequestBody;
    }
  }
}
