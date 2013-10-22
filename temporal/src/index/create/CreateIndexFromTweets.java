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

public class CreateIndexFromTweets {


  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.err.println("Usage: <tweet directory> <index directory location>");
      System.exit(-1);
    }

    Directory directory = FSDirectory.open(new File(args[1]));
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35, Collections.emptySet());
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
    
    File rootDir = new File(args[0]);
    Collection<File> files = FileUtils.listFiles(rootDir, new String[]{"raw.tsv"}, true);
    for(File file : files) {
      LineIterator iter = FileUtils.lineIterator(file);
      while(iter.hasNext()){
        String[] fields = iter.nextLine().split("\\t"); 
        Document document = new Document();
//        document.add(new Field("tid", fields[0], Field.Store.YES, Field.Index.NOT_ANALYZED));
//        document.add(new Field("timestamp", fields[1], Field.Store.YES, Field.Index.NOT_ANALYZED));
        document.add(new Field("content", fields[2], Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
        indexWriter.addDocument(document);
      }
      iter.close();
    }

    indexWriter.close();
    System.out.println("done!");
  }
}
