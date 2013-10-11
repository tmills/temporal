package index.create;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class CreateIndexFromTweets {

/*  Directory directory = null;
  Analyzer analyzer = null;
  IndexWriterConfig indexWriterConfig = null;
  IndexWriter indexWriter = null;
  String rawDirectory = null;
  
  public CreateIndexFromTweets(String indexLoc) throws IOException{
    // filter for files with extension .raw.tsv
    super(
        FileFilterUtils.andFileFilter(FileFilterUtils.fileFileFilter(),
                                      FileFilterUtils.suffixFileFilter(".raw.tsv")), -1);
    directory = FSDirectory.open(new File(indexLoc));
    analyzer = new StandardAnalyzer(Version.LUCENE_35);
    indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    indexWriter = new IndexWriter(directory, indexWriterConfig);
  }
  
  public void indexTweets(String rawDirectory) throws IOException{
    walk(new File(rawDirectory), new ArrayList<String>());
  }
  
  @Override
  protected void handleFile(File file, int depth, Collection results)
      throws IOException {
    super.handleFile(file, depth, results);
    BufferedReader reader = new BufferedReader(new FileInputStream(file));
    
  }
*/
  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.err.println("Usage: <tweet directory> <index directory location>");
      System.exit(-1);
    }

//    CreateIndexFromTweets fileWalker = new CreateIndexFromTweets(args[1]);
//    fileWalker.indexTweets(args[0]);
    Directory directory = FSDirectory.open(new File(args[1]));
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
    
    File rootDir = new File(args[0]);
    Collection<File> files = FileUtils.listFiles(rootDir, new String[]{"raw.tsv"}, true);
    for(File file : files) {
      LineIterator iter = FileUtils.lineIterator(file);
      while(iter.hasNext()){
        String[] fields = iter.nextLine().split("\\t"); 
        Document document = new Document();
        document.add(new Field("tid", fields[0], Field.Store.YES, Field.Index.NOT_ANALYZED));
        document.add(new Field("timestamp", fields[1], Field.Store.YES, Field.Index.NOT_ANALYZED));
        document.add(new Field("content", fields[2], Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
        indexWriter.addDocument(document);
      }
    }

    indexWriter.close();
    System.out.println("done!");
  }

}
