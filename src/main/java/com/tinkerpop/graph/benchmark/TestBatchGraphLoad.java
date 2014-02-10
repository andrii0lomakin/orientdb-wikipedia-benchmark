package com.tinkerpop.graph.benchmark;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test framework to load a very large graph ( Wikipedia links - download from http://downloads.dbpedia.org/3.6/en/page_links_en.nt.bz2 )
 * Outputs a benchmark file showing insert costs over time which can hopefully can be published and compared.
 * <p/>
 * A choice of Spring config file is used to initialize the choice of graph database implementation.
 *
 * @author MAHarwood
 */
public class TestBatchGraphLoad {

	String benchmarkResultsFilename;
	String inputTestDataFile;
	//Log progress every n records...
	int batchReportTime = 100000;
	//The name of a file which if found will stop the ingest at the next batch progress report time
	String stopFileName = "stop.txt";
	int numRecordsToLoad = 1000000; //set to a value >0 to limit the scope of the test

	GraphLoaderService graphLoaderService;

	/**
	 * @param args
	 * @throws java.io.IOException
	 * @throws java.io.FileNotFoundException
	 */
	public static void main(String[] args) throws Exception {
		String testFile = "/config/graphBeans.xml";
		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load(new ClassPathResource(testFile));
		TestBatchGraphLoad testRunner = (TestBatchGraphLoad) context.getBean("testRunner");
		testRunner.runTest();
	}

	public void runTest() throws Exception {
		final File inputDataFile = new File(inputTestDataFile);
		if (!inputDataFile.exists()) {
			System.out.println("Data file " + inputDataFile + " is absent. Will de downloaded from http://downloads.dbpedia.org/3.6/en/page_links_en.nt.bz2 .");

			String parent = inputDataFile.getParent();
			if (parent != null) {
				new File(parent).mkdirs();
			}

			final URL website = new URL("http://downloads.dbpedia.org/3.6/en/page_links_en.nt.bz2");

			try (final ReadableByteChannel rbc = Channels.newChannel(website.openStream())) {
				try (final FileOutputStream fos = new FileOutputStream(inputDataFile)) {
					fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
				}
			}

			System.out.println("Data file " + inputDataFile + " download was completed.");
		}

		final File benchmarkResultsFile = new File(benchmarkResultsFilename);
		if(!benchmarkResultsFile.exists()) {
			String parent = benchmarkResultsFile.getParent();
			if (parent != null)
				new File(parent).mkdirs();
		}

		PrintWriter benchmarkResultsLog = new PrintWriter(new FileOutputStream(benchmarkResultsFile), true);
		BufferedInputStream fis = new BufferedInputStream(new FileInputStream(inputDataFile));

		BZip2CompressorInputStream zis = new BZip2CompressorInputStream(fis);
		BufferedReader r = new BufferedReader(new InputStreamReader(zis));


		//Example line from Wikipedia file:
		//	<http://dbpedia.org/resource/Calf> <http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/resource/Veal> .
		Pattern p = Pattern.compile("<http://dbpedia.org/resource/([^>]*)> <[^>]*> <http://dbpedia.org/resource/([^>]*)> .");

		String line = r.readLine();
		int numRecordsProcessed = 0;
		long start = System.currentTimeMillis();

		benchmarkResultsLog.println("NumRecords,timeForLast" + batchReportTime + "Records(ms)");
		while (line != null) {
			Matcher m = p.matcher(line);
			if (m.matches()) {
				numRecordsProcessed++;
				if (numRecordsToLoad == numRecordsProcessed) {
					break;
				}
				String from = m.group(1);
				String to = m.group(2);
				if (!from.equals(to)) {
					graphLoaderService.addLink(from, to);
				}
				if (numRecordsProcessed % batchReportTime == 0) {
					long diff = System.currentTimeMillis() - start;
					System.out.println("Processed " + numRecordsProcessed + " records." +
									"Last batch in " + diff + " ms," +
									" last record links [" + from + "] to [" + to + "]");
					benchmarkResultsLog.println(numRecordsProcessed + "," + diff);
					if (stopFileName != null) {
						File stopFile = new File(stopFileName);
						if (stopFile.exists()) {
							stopFile.delete();
							break;
						}
					}
					start = System.currentTimeMillis();
				}
			}
			line = r.readLine();
		}
		System.out.println("Issuing close request");
		graphLoaderService.close();
		long diff = System.currentTimeMillis() - start;
		benchmarkResultsLog.println(numRecordsProcessed + "," + diff);

		benchmarkResultsLog.close();
		fis.close();
	}

	public String getBenchmarkResultsFilename() {
		return benchmarkResultsFilename;
	}

	public void setBenchmarkResultsFilename(String benchmarkResultsFilename) {
		this.benchmarkResultsFilename = benchmarkResultsFilename;
	}

	public String getInputTestDataFile() {
		return inputTestDataFile;
	}

	public void setInputTestDataFile(String inputTestDataFile) {
		this.inputTestDataFile = inputTestDataFile;
	}

	public int getBatchReportTime() {
		return batchReportTime;
	}

	public void setBatchReportTime(int batchReportTime) {
		this.batchReportTime = batchReportTime;
	}

	public String getStopFileName() {
		return stopFileName;
	}

	public void setStopFileName(String stopFileName) {
		this.stopFileName = stopFileName;
	}

	public GraphLoaderService getGraphLoaderService() {
		return graphLoaderService;
	}

	public void setGraphLoaderService(GraphLoaderService graphLoaderService) {
		this.graphLoaderService = graphLoaderService;
	}

	public int getNumRecordsToLoad() {
		return numRecordsToLoad;
	}

	public void setNumRecordsToLoad(int numRecordsToLoad) {
		this.numRecordsToLoad = numRecordsToLoad;
	}
}
