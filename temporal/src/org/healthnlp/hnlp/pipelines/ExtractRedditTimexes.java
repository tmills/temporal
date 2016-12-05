package org.healthnlp.hnlp.pipelines;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.ctakes.temporal.ae.BackwardsTimeAnnotator;
import org.apache.ctakes.temporal.pipelines.TemporalExtractionPipeline_ImplBase;
import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

public class ExtractRedditTimexes extends TemporalExtractionPipeline_ImplBase {

  public static final String JSON_VIEW = "json_view";
  
  static interface Options {

    @Option(
        shortName = "i",
        description = "specify the path to the directory containing the clinical notes to be processed")
    public File getInputDirectory();
    
    @Option(
        shortName = "o",
        description = "specify the path to the directory where the output xmi files are to be saved")
    public String getOutputDirectory();
  }
  
  
  public static void main(String[] args) throws Exception{
    Options options = CliFactory.parseArguments(Options.class, args);

    for(File inputFile : options.getInputDirectory().listFiles()){
      String diseaseName = inputFile.getName();
      
      CollectionReader collectionReader = CollectionReaderFactory.createReader(RedditJsonCollectionReader.class, RedditJsonCollectionReader.PARAM_INPUT_FILE, inputFile);

      AggregateBuilder aggregateBuilder = new AggregateBuilder(); 

      aggregateBuilder.add(getPreprocessorAggregateBuilder().createAggregateDescription());
      aggregateBuilder.add(BackwardsTimeAnnotator.createAnnotatorDescription("/org/apache/ctakes/temporal/ae/timeannotator/model.jar"));
      //"/org/apache/ctakes/temporal/ae/timeannotator/model.jar"));
      File outputDir = new File(options.getOutputDirectory(), diseaseName);
      if(!outputDir.exists()){
        outputDir.mkdir();
      }
      AnalysisEngine xWriter = getXMIWriter(outputDir.getAbsolutePath());

      try{
        SimplePipeline.runPipeline(
            collectionReader,
            aggregateBuilder.createAggregate(),
            xWriter);
      }catch(Exception e){
        System.err.println("Error while processing a document with filename: " + inputFile.getName());
      }
    }
  }
  
  public static class RedditJsonCollectionReader extends CasCollectionReader_ImplBase {

    public static final String PARAM_INPUT_FILE = "inputFile";
    @ConfigurationParameter(name = PARAM_INPUT_FILE)
    private File inputFile;
    
    private BufferedReader reader = null;
    private String nextLine = null;
    private String diseaseName = null;
    private int lineNum = 0;
    
    private static final String xml10pattern = "[^"
        + "\u0009\r\n"
        + "\u0020-\uD7FF"
        + "\uE000-\uFFFD"
        + "\ud800\udc00-\udbff\udfff"
        + "]";
    
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
      super.initialize(context);
      
      diseaseName = inputFile.getName().split("-")[0];
      try {
        reader = new BufferedReader(new FileReader(inputFile));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        throw new ResourceInitializationException();
      }
    }
    
    @Override
    public void getNext(CAS aCAS) throws IOException, CollectionException {
      ObjectMapper m = new ObjectMapper();
      JsonNode rootNode = m.readTree(nextLine);
      String text = replaceText(rootNode.path("selftext").textValue()); //.replaceAll(xml10pattern, "");
      
//      String text = gson.fromJson(nextLine, String.class);
//      JsonReader rdr = Json.createReader(new StringReader(nextLine));
//      JsonObject obj = rdr.readObject();
//      String text = obj.getString("selftext");
      
      JCas jcas;
      try {
        jcas = aCAS.getJCas();
        jcas.setDocumentText(text);
        DocumentID docId = new DocumentID(jcas);
        docId.setDocumentID(diseaseName + "." + lineNum + ".txt");
        docId.addToIndexes();
      } catch (CASException e) {
        e.printStackTrace();
        throw new CollectionException();
      }
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
      nextLine = reader.readLine();
      lineNum++;
      return nextLine != null;
    }

    @Override
    public Progress[] getProgress() {
      return new Progress[]{new ProgressImpl(lineNum, Integer.MAX_VALUE, "lines")};
    }

    @Override
    public void close() throws IOException {
      reader.close();      
    }    
  }

  /*
   * http://stackoverflow.com/a/11672807
   */
  private static String replaceText(String text){
    if (null == text || text.isEmpty()) {
      return text;
    }
    final int len = text.length();
    char current = 0;
    int codePoint = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      current = text.charAt(i);
      boolean surrogate = false;
      if (Character.isHighSurrogate(current)
          && i + 1 < len && Character.isLowSurrogate(text.charAt(i + 1))) {
        surrogate = true;
        codePoint = text.codePointAt(i++);
      } else {
        codePoint = current;
      }
      if ((codePoint == 0x9) || (codePoint == 0xA) || (codePoint == 0xD)
          || ((codePoint >= 0x20) && (codePoint <= 0xD7FF))
          || ((codePoint >= 0xE000) && (codePoint <= 0xFFFD))
          || ((codePoint >= 0x10000) && (codePoint <= 0x10FFFF))) {
        if(!surrogate) sb.append(current);
      }else{
        System.err.println("Illegal character!");
      }
    }
    return sb.toString();
  }
}
