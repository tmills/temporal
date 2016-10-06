package index.create;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.google.common.collect.Lists;

public class CreateIndexFromTweets {

  private String fileOrDir = null;
  private String indexLoc = null;
  
  public CreateIndexFromTweets(String fileOrDir, String indexLoc){
    this.fileOrDir = fileOrDir;
    this.indexLoc = indexLoc;
  }

  public void build() throws IOException{
    Directory directory = FSDirectory.open(new File(indexLoc));
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35, Collections.emptySet());
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
    
    File inputFile = new File(fileOrDir);
    Collection<File> files = null;
    if(inputFile.isDirectory()){
      files = FileUtils.listFiles(inputFile, new String[]{"raw.tsv"}, true);
    }else{
      files = Lists.newArrayList();
      files.add(inputFile);
    }
    for(File file : files) {
      System.out.println("Reading file: " + file.getName());
      LineIterator iter = FileUtils.lineIterator(file);
      while(iter.hasNext()){
        String[] fields = iter.nextLine().split("\\t", -1); 
        Document document = new Document();
        document.add(new Field("tid", fields[0], Field.Store.YES, Field.Index.NOT_ANALYZED));
        document.add(new Field("timestamp", fields[1], Field.Store.YES, Field.Index.NOT_ANALYZED));
        if(fields[2].startsWith("RT")) continue;
        document.add(new Field("content", fields[2], Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
        document.add(new Field("user", fields[3], Field.Store.YES, Field.Index.NOT_ANALYZED));
        // fields[4] is user description -- not important
        document.add(new Field("language", fields[5], Field.Store.YES, Field.Index.NOT_ANALYZED));
        document.add(new Field("location", fields[6], Field.Store.YES, Field.Index.NOT_ANALYZED));
        // fields[7] is user time zone
        document.add(new Field("geom", fields[8]+"_"+fields[9], Field.Store.YES, Field.Index.NOT_ANALYZED));
        // fields[10] is geom_src???
        indexWriter.addDocument(document);
      }
      iter.close();
    }

    indexWriter.close();
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.err.println("Usage: <tweet directory> <index directory location>");
      System.exit(-1);
    }

    CreateIndexFromTweets indexBuilder = new CreateIndexFromTweets(args[0], args[1]);
    indexBuilder.build();
    System.out.println("done!");
  }
}
