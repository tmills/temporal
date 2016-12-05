package index.search;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;

import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

public class ExtractTweetIDs {

  public interface Options {
    @Option
    String getTweetFile();
    
    @Option
    String getOutputFile();
    
    @Option(defaultValue="/home/tmill/mnt/rc-pub/resources/tweet_index")
    String getIndex();
    
  }
  public static void main(String[] args) throws CorruptIndexException, IOException {
    Options options = CliFactory.parseArguments(Options.class, args);
    final int maxHits = 5;
    final String searchField = "content";
    final String idField = "tid";
    final String indexLocation = options.getIndex();
    final File tweetFile = new File(options.getTweetFile());
    final File outFile = new File(options.getOutputFile());
    
    DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexLocation)));
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);

    PrintWriter out = new PrintWriter(outFile);
    Scanner scanner = new Scanner(tweetFile);
    int count = 0;
    while(scanner.hasNextLine()){
      String tweet = scanner.nextLine().trim().toLowerCase();

      try{
        Query q = new QueryBuilder(new StandardAnalyzer()).createPhraseQuery(searchField, tweet.replaceAll("[\\!\\?\"()]", ""));
        TopDocs topDocs = indexSearcher.search(q, maxHits);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;     

        Document document = indexSearcher.doc(scoreDocs[0].doc);
        String tid = document.get(idField);
        String doc = document.get(searchField).replaceAll("[^\\x20-\\x7E]", "").trim().toLowerCase();
        if(doc.equals(tweet)){
          out.printf("%s\t%s\n", doc, tid);
          count++;
        }
        else{
          System.err.printf("Error: Search does not match retrieved tweet: %s :: %s\n", tweet, doc);
        }
      }catch(Exception e){
        System.err.printf("Error: Exception caught trying to find this tweet: %s\n", tweet);        
      }
      if(count > 1000) break;
    }
    System.out.println("Completed with " + count + " tweets printed.");
    scanner.close();
    out.close();
    indexReader.close();
  }

}
