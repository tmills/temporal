package index.create;

import java.io.File;
import java.io.IOException;
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
import org.json.JSONObject;

public class CreateIndexFromReddit {
  private String fileOrDir = null;
  private String indexLoc = null;

  private static final String AUTHOR_KEY = "author";
  private static final String TITLE_KEY = "title";
  private static final String SUBREDDIT_KEY = "subreddit";
  
  public CreateIndexFromReddit(String fileOrDir, String indexLoc){
    this.fileOrDir = fileOrDir;
    this.indexLoc = indexLoc;
  }
  
  public void build() throws IOException{
    Directory directory = FSDirectory.open(new File(indexLoc));
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35, Collections.emptySet());
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
    
//    File inputFile = new File(fileOrDir);
//    for(File file : files) {
    File file = new File(fileOrDir);
      System.out.println("Reading file: " + file.getName());
      LineIterator iter = FileUtils.lineIterator(file);
      while(iter.hasNext()){
        JSONObject obj = new JSONObject(iter.nextLine().trim());
        String selfText = obj.getString("selftext");
        
        if(!selfText.equals("")){
          Document document = new Document();
          document.add(new Field("id", obj.getString("id"), Field.Store.YES, Field.Index.NOT_ANALYZED));
          document.add(new Field("timestamp", String.valueOf(obj.getInt("created_utc")), Field.Store.YES, Field.Index.NOT_ANALYZED));
          document.add(new Field("title", obj.getString(TITLE_KEY), Field.Store.YES, Field.Index.ANALYZED));
          document.add(new Field("content", selfText, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
          document.add(new Field("user", obj.getString(AUTHOR_KEY), Field.Store.YES, Field.Index.NOT_ANALYZED));
          document.add(new Field("subreddit", obj.optString(SUBREDDIT_KEY, "None"), Field.Store.YES, Field.Index.NOT_ANALYZED));

          indexWriter.addDocument(document);
        }
      }
      iter.close();
//    }

    indexWriter.close();
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.err.println("Usage: <reddit file> <index directory location>");
      System.exit(-1);
    }

    CreateIndexFromReddit indexBuilder = new CreateIndexFromReddit(args[0], args[1]);
    indexBuilder.build();
    System.out.println("done!");
  }
}
