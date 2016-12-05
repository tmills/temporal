package index.create;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

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
import org.json.JSONObject;

public class CreateIndexFromReddit {
  private String fileOrDir = null;
  private String indexLoc = null;

  private static final String AUTHOR_KEY = "author";
  private static final String TITLE_KEY = "title";
  private static final String SUBREDDIT_KEY = "subreddit";
  private static final String PARENT_ID_KEY = "parent_id";
  private static final String LINK_ID_KEY = "link_id";
  public CreateIndexFromReddit(String fileOrDir, String indexLoc){
    this.fileOrDir = fileOrDir;
    this.indexLoc = indexLoc;
  }
  
  public void build() throws IOException{
    Directory directory = FSDirectory.open(Paths.get(indexLoc));
    Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
    File[] files = null;
    
    File inputFile = new File(fileOrDir);
    if(inputFile.isDirectory()){
      files = inputFile.listFiles();
    }else{
      files = new File[]{ inputFile };
    }
    
    for(File file : files) {
      System.out.println("Reading file: " + file.getName());
      LineIterator iter = FileUtils.lineIterator(file);
      while(iter.hasNext()){
        JSONObject obj = new JSONObject(iter.nextLine().trim());
        String selfText = obj.optString("selftext", "");
        String commentBody = obj.optString("body", "");
        
        if(selfText.equals("")){
          if(!commentBody.equals("")){
            Document document = new Document();
            document.add(new StringField("id", obj.getString("id"), Field.Store.YES));
            document.add(new StringField("timestamp", String.valueOf(obj.getInt("created_utc")), Field.Store.YES));
            document.add(new StringField("subreddit", obj.optString(SUBREDDIT_KEY, "None"), Field.Store.YES));
            document.add(new StringField("user", obj.getString(AUTHOR_KEY), Field.Store.YES));
            document.add(new StringField("timestamp", String.valueOf(obj.getInt("created_utc")), Field.Store.YES));
            document.add(new StringField("parent_id", obj.optString(PARENT_ID_KEY, "None"), Field.Store.YES));
            document.add(new StringField("link_id", obj.optString(LINK_ID_KEY, "None"), Field.Store.YES));
            document.add(new TextField("content", commentBody, Field.Store.YES));
            indexWriter.addDocument(document);
          }
        }else{
          // self text is not empty so it's a self post with some content
          Document document = new Document();
          document.add(new StringField("id", obj.getString("id"), Field.Store.YES));
          document.add(new StringField("timestamp", String.valueOf(obj.getInt("created_utc")), Field.Store.YES));
          document.add(new StringField("title", obj.optString(TITLE_KEY, "N/A"), Field.Store.YES));
          document.add(new TextField("content", selfText, Field.Store.YES));
          document.add(new StringField("user", obj.getString(AUTHOR_KEY), Field.Store.YES));
          document.add(new StringField("subreddit", obj.optString(SUBREDDIT_KEY, "None"), Field.Store.YES));

          indexWriter.addDocument(document);
        }
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
      System.err.println("Usage: <reddit file> <index directory location>");
      System.exit(-1);
    }

    CreateIndexFromReddit indexBuilder = new CreateIndexFromReddit(args[0], args[1]);
    indexBuilder.build();
    System.out.println("done!");
  }
}
