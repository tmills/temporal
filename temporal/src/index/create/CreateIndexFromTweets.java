package index.create;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.google.common.collect.Lists;

public class CreateIndexFromTweets {

  private String fileOrDir = null;
  private String indexLoc = null;
  
  public CreateIndexFromTweets(String fileOrDir, String indexLoc){
    this.fileOrDir = fileOrDir;
    this.indexLoc = indexLoc;
  }

  public void build() throws IOException{
    Directory directory = FSDirectory.open(Paths.get(indexLoc));
    Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
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
        document.add(new StringField("tid", fields[0], Field.Store.YES));
        document.add(new StringField("timestamp", fields[1], Field.Store.YES));
        if(fields[2].startsWith("RT")) continue;
        document.add(new TextField("content", fields[2], Field.Store.YES));
        document.add(new StringField("user", fields[3], Field.Store.YES));
        // fields[4] is user description -- not important
        document.add(new StringField("language", fields[5], Field.Store.YES));
        document.add(new StringField("location", fields[6], Field.Store.YES));
        // fields[7] is user time zone
        document.add(new StringField("geom", fields[8]+"_"+fields[9], Field.Store.YES));
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
